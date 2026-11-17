"""End-to-end tests: real origin + proxy processes, real HTTP.

Run from the repo root:  python3 -m pytest tests/ -v
Requires: fastapi, uvicorn, aiohttp (app deps) + httpx, pytest (test deps).
"""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
MIB = 1024 * 1024
PKG_BIG = "pkg-32m.bin"
PKG_SMALL = "pkg-1m.bin"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_health(url: str, proc: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"server at {url} did not become healthy")


def start_server(appdir: str, port: int, env_extra: dict, log: Path,
                 control: Path) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "tests" / "run_server.py"),
         str(ROOT / appdir), str(port), str(control)],
        env=env,
        stdout=log.open("ab"),
        stderr=subprocess.STDOUT,
    )
    wait_health(f"http://127.0.0.1:{port}/healthz", proc)
    return proc


def stop(proc: subprocess.Popen, control: Path) -> None:
    """Ask the server to exit via its control file (signals are blocked here)."""
    if proc.poll() is None:
        control.touch(exist_ok=True)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except PermissionError:
                pass
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                pass


def client(timeout_read: float = 120.0) -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(5.0, read=timeout_read, write=30.0, pool=5.0))


def download_bytes(base: str, path: str) -> bytes:
    with client() as c:
        r = c.get(f"{base}/packages/{path}")
        r.raise_for_status()
        return r.content


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def origin_stats(origin_url: str) -> dict:
    with client() as c:
        return c.get(f"{origin_url}/control/stats").json()


def origin_reset(origin_url: str) -> None:
    with client() as c:
        c.post(f"{origin_url}/control/reset")


def set_behavior(origin_url: str, delay_ms: int = 0, truncate: int | None = None,
                 chunk_size: int = 256 * 1024, wrong_sha: bool = False) -> None:
    with client() as c:
        r = c.post(
            f"{origin_url}/control/behavior",
            json={
                "chunk_delay_ms": delay_ms,
                "truncate_after_bytes": truncate,
                "chunk_size": chunk_size,
                "send_wrong_sha256": wrong_sha,
            },
        )
        r.raise_for_status()


def replace_content(origin_url: str, path: str, size: int) -> dict:
    with client() as c:
        r = c.post(f"{origin_url}/control/content/{path}", params={"size": size})
        r.raise_for_status()
        return r.json()


def wait_until(cond, timeout: float = 10.0, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        time.sleep(interval)
    return False


@pytest.fixture(scope="session")
def origin(tmp_path_factory):
    pkg_dir = tmp_path_factory.mktemp("pkgs")
    log_dir = tmp_path_factory.mktemp("logs")
    log = log_dir / "origin.log"
    control = log_dir / "origin.stop"
    port = free_port()
    proc = start_server("origin", port, {"PKG_DIR": str(pkg_dir)}, log, control)
    yield SimpleNamespace(url=f"http://127.0.0.1:{port}", port=port, dir=pkg_dir, log=log)
    stop(proc, control)


class ProxyRunner:
    def __init__(self, origin_port: int, cache_dir: Path, log: Path, control: Path):
        self.origin_port = origin_port
        self.cache_dir = cache_dir
        self.log = log
        self.control = control
        self.proc: subprocess.Popen | None = None
        self.url: str = ""

    def start(self) -> None:
        port = free_port()
        if self.control.exists():
            self.control.unlink()
        self.proc = start_server(
            "proxy",
            port,
            {
                "ORIGIN_BASE_URL": f"http://127.0.0.1:{self.origin_port}",
                "CACHE_DIR": str(self.cache_dir),
            },
            self.log,
            self.control,
        )
        self.url = f"http://127.0.0.1:{port}"

    def restart(self) -> None:
        stop(self.proc, self.control)
        self.start()

    def stop(self) -> None:
        if self.proc:
            stop(self.proc, self.control)

    def rss_bytes(self) -> int:
        with open(f"/proc/{self.proc.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("VmRSS not found")


@pytest.fixture()
def proxy(origin, tmp_path):
    runner = ProxyRunner(origin.port, tmp_path / "cache", tmp_path / "proxy.log",
                         tmp_path / "proxy.stop")
    runner.start()
    yield runner
    runner.stop()


# --------------------------------------------------------------------- #
# tests                                                                  #
# --------------------------------------------------------------------- #

def test_basic_download_then_cache_hit(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    stats = origin_stats(origin.url)
    assert stats["paths"][PKG_SMALL] == 1
    assert sha256(body1) == stats["packages"][PKG_SMALL]["sha256"]

    with client() as c:
        r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r2.headers["x-cache"] == "HIT"
    assert r2.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no second fetch


def test_concurrent_miss_single_fill(proxy, origin):
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~1.2s fill window
    try:
        origin_reset(origin.url)
        with ThreadPoolExecutor(8) as ex:
            bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(8)))
        assert origin_stats(origin.url)["total_hits"] == 1
        assert all(b == bodies[0] for b in bodies)
        assert len(bodies[0]) == MIB
    finally:
        set_behavior(origin.url)


def test_first_chunk_arrives_before_full_package(proxy, origin):
    # 32MiB at 100ms per 256KiB chunk -> full fill takes ~13s.
    set_behavior(origin.url, delay_ms=100, chunk_size=256 * 1024)
    try:
        t0 = time.monotonic()
        with client(timeout_read=60).stream("GET", f"{proxy.url}/packages/{PKG_BIG}") as r:
            first = next(r.iter_bytes(256 * 1024))
            ttfb = time.monotonic() - t0
            assert len(first) > 0
        assert ttfb < 3.0, f"first chunk took {ttfb:.2f}s"
    finally:
        set_behavior(origin.url)
        with client() as c:
            c.post(f"{proxy.url}/purge/{PKG_BIG}")


def test_slow_client_blocks_nobody(proxy, origin):
    # Fill pace ~20ms/256KiB -> ~2.6s for 32MiB; slow client crawls far slower.
    set_behavior(origin.url, delay_ms=20, chunk_size=256 * 1024)
    try:
        origin_reset(origin.url)
        result: dict = {}

        def slow_reader():
            with client(timeout_read=300).stream(
                "GET", f"{proxy.url}/packages/{PKG_BIG}"
            ) as r:
                got = 0
                for chunk in r.iter_bytes(64 * 1024):
                    got += len(chunk)
                    time.sleep(0.2)
                    if got >= 5 * 64 * 1024:
                        break
                result["slow_got"] = got

        t = threading.Thread(target=slow_reader)
        t.start()
        time.sleep(0.3)  # slow client attaches first
        t0 = time.monotonic()
        fast = download_bytes(proxy.url, PKG_BIG)
        fast_dur = time.monotonic() - t0
        t.join(60)

        assert len(fast) == 32 * MIB
        assert fast_dur < 15, f"fast client dragged to {fast_dur:.1f}s by slow client"
        assert origin_stats(origin.url)["total_hits"] == 1  # shared single fill

        # Fill completed in the background despite the slow client leaving.
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == origin_stats(origin.url)["packages"][PKG_BIG]["sha256"]
    finally:
        set_behavior(origin.url)


def test_purge_refills_and_serves_new_content(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    sha1 = sha256(body1)

    with client() as c:
        new = c.post(f"{origin.url}/control/content/{PKG_SMALL}").json()
    assert new["sha256"] != sha1

    # Still cached: old content, no new origin fetch.
    assert sha256(download_bytes(proxy.url, PKG_SMALL)) == sha1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    with client() as c:
        pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    assert pr.status_code == 200 and pr.json()["was_cached"] is True

    # First download after purge re-fills exactly once; followers share it.
    with ThreadPoolExecutor(6) as ex:
        bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(6)))
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
    assert all(sha256(b) == new["sha256"] for b in bodies)


def test_purge_during_fill_keeps_old_download_alive(proxy, origin):
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)  # ~1s fill
    try:
        origin_reset(origin.url)
        result: dict = {}

        def reader_a():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/packages/{PKG_SMALL}"
            ) as r:
                chunks = []
                for chunk in r.iter_bytes(64 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.03)
                result["a"] = b"".join(chunks)

        ta = threading.Thread(target=reader_a)
        ta.start()
        time.sleep(0.3)  # A is mid-fill on generation 1

        with client() as c:
            pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        assert pr.json()["was_cached"] is True

        body_b = download_bytes(proxy.url, PKG_SMALL)  # triggers generation 2
        ta.join(30)

        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2  # old fill + new fill
        assert len(result["a"]) == MIB  # old download completed in full
        assert sha256(result["a"]) == sha256(body_b)
    finally:
        set_behavior(origin.url)


def test_upstream_disconnect_fails_all_and_caches_nothing(proxy, origin):
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024, truncate=200_000)
    try:
        origin_reset(origin.url)

        def expect_failure():
            with pytest.raises(httpx.HTTPError):
                download_bytes(proxy.url, PKG_SMALL)

        with ThreadPoolExecutor(4) as ex:
            list(ex.map(lambda _: expect_failure(), range(4)))

        assert origin_stats(origin.url)["total_hits"] == 1  # single shared fill
        with client() as c:
            status = c.get(f"{proxy.url}/cache/status").json()
        assert PKG_SMALL not in status
        leftovers = list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert leftovers == [], f"half package left behind: {leftovers}"
    finally:
        set_behavior(origin.url)

    # Origin healthy again -> retry succeeds and caches.
    body = download_bytes(proxy.url, PKG_SMALL)
    assert len(body) == MIB
    assert origin_stats(origin.url)["total_hits"] == 2


def test_restart_keeps_complete_cache_and_cleans_partials(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    stray = proxy.cache_dir / f"{PKG_SMALL}.part-deadbeef"
    stray.write_bytes(b"x" * 100)
    orphan_meta = proxy.cache_dir / "ghost.meta"
    orphan_meta.write_text("{}")

    proxy.restart()

    with client() as c:
        r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r.headers["x-cache"] == "HIT"
    assert r.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # served from disk cache
    assert not stray.exists()
    assert not orphan_meta.exists()


def test_memory_does_not_grow_with_package_size(proxy, origin):
    rss0 = proxy.rss_bytes()
    body = download_bytes(proxy.url, PKG_BIG)
    assert len(body) == 32 * MIB
    time.sleep(0.5)
    rss1 = proxy.rss_bytes()
    delta = rss1 - rss0
    print(f"\nproxy RSS delta while streaming 32MiB: {delta / MIB:.1f} MiB")
    # Buffering the whole package would cost >=32MiB; streaming stays far below.
    assert delta < 24 * MIB


def test_missing_package_returns_404_and_no_negative_cache(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/nope.bin")
        r2 = c.get(f"{proxy.url}/packages/nope.bin")
    assert r1.status_code == 404 and r2.status_code == 404
    assert origin_stats(origin.url)["paths"]["nope.bin"] == 2


def test_path_traversal_rejected(proxy):
    with client() as c:
        r = c.get(f"{proxy.url}/packages/..%2f..%2fetc%2fpasswd")
    assert r.status_code in (400, 404)


# --------------------------------------------------------------------- #
# download/replace interleaving                                          #
# --------------------------------------------------------------------- #

def test_origin_download_pinned_to_one_version_during_replace(origin):
    """A GET accepted before a replace finishes the OLD version (body, length
    and digest all from that version); a GET after it reads the NEW one."""
    key = "pkg-replace-race.bin"
    old = replace_content(origin.url, key, MIB)
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024)  # ~0.8s stream
    try:
        result: dict = {}

        def reader():
            with client(timeout_read=60).stream(
                "GET", f"{origin.url}/packages/{key}"
            ) as r:
                result["headers"] = dict(r.headers)
                chunks = []
                for chunk in r.iter_bytes(64 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.02)  # stay mid-stream across the replace
                result["body"] = b"".join(chunks)

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.25)  # reader is mid-download on the old version

        new = replace_content(origin.url, key, 2 * MIB)  # different size AND sha
        assert new["sha256"] != old["sha256"]
        t.join(30)

        # The accepted download completed the old version, self-consistently.
        assert len(result["body"]) == MIB
        assert result["headers"]["content-length"] == str(MIB)
        assert result["headers"]["x-package-sha256"] == old["sha256"]
        assert sha256(result["body"]) == old["sha256"]

        # A GET accepted after the replace reads the new version.
        with client() as c:
            r = c.get(f"{origin.url}/packages/{key}")
        assert len(r.content) == 2 * MIB
        assert r.headers["x-package-sha256"] == new["sha256"]
        assert sha256(r.content) == new["sha256"]
        assert origin_stats(origin.url)["open_downloads"] == 0
    finally:
        set_behavior(origin.url)


def test_origin_every_response_is_one_consistent_version(origin):
    """Under repeated replaces, every response's body matches ITS headers."""
    key = "pkg-consistency.bin"
    replace_content(origin.url, key, MIB)
    set_behavior(origin.url, delay_ms=10, chunk_size=64 * 1024)  # ~0.2s streams
    errors: list = []
    try:
        def reader():
            try:
                with client(timeout_read=60).stream(
                    "GET", f"{origin.url}/packages/{key}"
                ) as r:
                    body = b"".join(r.iter_bytes(64 * 1024))
                if sha256(body) != r.headers["x-package-sha256"]:
                    errors.append("body digest != X-Package-Sha256 header")
                if len(body) != int(r.headers["content-length"]):
                    errors.append("body length != Content-Length header")
            except Exception as exc:  # noqa: BLE001 - any failure is a defect
                errors.append(f"{type(exc).__name__}: {exc}")

        for i in range(6):
            threads = [threading.Thread(target=reader) for _ in range(4)]
            for t in threads:
                t.start()
            time.sleep(0.05)  # readers are mid-stream when the replace lands
            replace_content(origin.url, key, MIB + (i + 1) * 12345)
            for t in threads:
                t.join(30)
        assert errors == []
        assert wait_until(lambda: origin_stats(origin.url)["open_downloads"] == 0)
    finally:
        set_behavior(origin.url)


def test_proxy_fill_stays_consistent_when_origin_replaces_mid_fill(proxy, origin):
    """A proxy fill that straddles an origin replace caches one consistent
    (old) version; after purge the next fill fetches the new version."""
    key = "pkg-midfill-replace.bin"
    old = replace_content(origin.url, key, MIB)
    set_behavior(origin.url, delay_ms=40, chunk_size=64 * 1024)  # ~0.7s fill
    try:
        result: dict = {}

        def reader():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/packages/{key}"
            ) as r:
                result["headers"] = dict(r.headers)
                result["body"] = b"".join(r.iter_bytes(64 * 1024))

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.3)  # proxy fill is mid-flight on the old version
        new = replace_content(origin.url, key, 2 * MIB)
        t.join(30)

        # The fill read one consistent old version; the proxy cached it.
        assert len(result["body"]) == MIB
        assert result["headers"]["x-package-sha256"] == old["sha256"]
        assert sha256(result["body"]) == old["sha256"]
        with client() as c:
            r2 = c.get(f"{proxy.url}/packages/{key}")
        assert r2.headers["x-cache"] == "HIT"
        assert sha256(r2.content) == old["sha256"]

        # After purge, the next download fills the new version, consistently.
        with client() as c:
            c.post(f"{proxy.url}/purge/{key}")
            r3 = c.get(f"{proxy.url}/packages/{key}")
        assert len(r3.content) == 2 * MIB
        assert sha256(r3.content) == new["sha256"]
        assert r3.headers["x-package-sha256"] == new["sha256"]
    finally:
        set_behavior(origin.url)


# --------------------------------------------------------------------- #
# resource release on client disconnect                                  #
# --------------------------------------------------------------------- #

def test_origin_disconnect_releases_file_resources(origin):
    key = "pkg-origin-disconnect.bin"
    replace_content(origin.url, key, 4 * MIB)
    open_downloads = lambda: origin_stats(origin.url)["open_downloads"]  # noqa: E731
    assert open_downloads() == 0

    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024)  # ~3s stream
    try:
        # Mid-stream disconnect. (Keep the iterator referenced: dropping it
        # lets httpx close the connection immediately.)
        with client().stream("GET", f"{origin.url}/packages/{key}") as r:
            it = r.iter_bytes(64 * 1024)
            first = next(it)
            assert len(first) > 0
            assert wait_until(lambda: open_downloads() == 1, timeout=5)
        assert wait_until(lambda: open_downloads() == 0, timeout=5), (
            "mid-stream disconnect did not release the download"
        )

        # Disconnect before the first chunk (server still in pre-chunk sleep).
        set_behavior(origin.url, delay_ms=600, chunk_size=64 * 1024)
        with client().stream("GET", f"{origin.url}/packages/{key}"):
            pass  # close immediately, before any body byte
        assert wait_until(lambda: open_downloads() == 0, timeout=5), (
            "pre-first-chunk disconnect did not release the download"
        )
    finally:
        set_behavior(origin.url)


def test_proxy_disconnect_releases_reader(proxy, origin):
    key = "pkg-proxy-disconnect.bin"
    replace_content(origin.url, key, 4 * MIB)
    set_behavior(origin.url, delay_ms=30, chunk_size=64 * 1024)  # ~2s fill
    try:
        def readers() -> int:
            with client() as c:
                status = c.get(f"{proxy.url}/cache/status").json()
            return status.get(key, {}).get("readers", 0)

        with client().stream("GET", f"{proxy.url}/packages/{key}") as r:
            it = r.iter_bytes(64 * 1024)
            first = next(it)
            assert len(first) > 0
        assert wait_until(lambda: readers() == 0, timeout=5), (
            "proxy reader slot not released after disconnect"
        )
        # The fill still completes in the background and caches the package.
        assert wait_until(
            lambda: origin_stats(origin.url)["open_downloads"] == 0, timeout=15
        )
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{key}")
        assert r.headers["x-cache"] == "HIT"
        assert len(r.content) == 4 * MIB
    finally:
        set_behavior(origin.url)


def test_proxy_rejects_digest_mismatch(proxy, origin):
    """If the origin ever sends a digest header that does not match the body,
    the proxy fails every attached download and caches nothing."""
    key = "pkg-sha-mismatch.bin"
    replace_content(origin.url, key, MIB)
    set_behavior(origin.url, wrong_sha=True)
    try:
        origin_reset(origin.url)
        with pytest.raises(httpx.HTTPError):
            download_bytes(proxy.url, key)
        assert origin_stats(origin.url)["paths"][key] == 1  # single fill
        with client() as c:
            status = c.get(f"{proxy.url}/cache/status").json()
        assert key not in status
        assert list(proxy.cache_dir.glob(f"{key}*")) == []  # nothing cached
    finally:
        set_behavior(origin.url)

    # Origin healthy again -> retry succeeds and caches the real digest.
    body = download_bytes(proxy.url, key)
    assert sha256(body) == origin_stats(origin.url)["packages"][key]["sha256"]
    with client() as c:
        r = c.get(f"{proxy.url}/packages/{key}")
    assert r.headers["x-cache"] == "HIT"
