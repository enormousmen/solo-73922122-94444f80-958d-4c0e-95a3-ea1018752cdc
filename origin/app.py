"""Controllable local package origin used to demo/test the caching proxy.

Serves complete packages from PKG_DIR (a 32 MiB and a 1 MiB package are
generated on first boot). Control endpoints let you replace content, add
per-chunk delay, cut the stream mid-body, send a bogus digest header (fault
injection), and inspect how many times the proxy fetched.

Version consistency
-------------------
Package replacement is atomic: the new content is written to a temp file and
``os.replace`` swaps it in; the digest of the new inode is registered *before*
the swap becomes visible. Every GET pins its version at accept time: it opens
the package file, takes ``Content-Length`` from ``fstat`` of that open file
and ``X-Package-Sha256`` from the per-inode registry — so body, length and
digest of one response always describe the same version.

* A GET accepted after a replace opens the new file -> serves the new version.
* A download accepted before a replace keeps reading its already-open file
  descriptor; the kernel keeps the unlinked old inode alive until the last
  fd closes, so it finishes the old version, undisturbed.
* When a download ends — completed, aborted, or the client disconnects
  (before the first chunk or mid-body) — its fd and registry slot are
  released exactly once by a ``finally`` around the whole ASGI response.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import re
import threading
import uuid
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, Response
from pydantic import BaseModel, Field

PKG_DIR = Path(os.environ.get("PKG_DIR", "./packages")).resolve()
DEFAULT_PACKAGES = {
    "pkg-32m.bin": 32 * 1024 * 1024,
    "pkg-1m.bin": 1 * 1024 * 1024,
}

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")

behavior = {
    "chunk_delay_ms": 0,
    "truncate_after_bytes": None,  # int | None
    "chunk_size": 256 * 1024,
    "send_wrong_sha256": False,  # fault injection: digest header != body
}
hits = {"total": 0, "paths": {}}


class UpstreamAbort(Exception):
    """Raised mid-stream to simulate an upstream connection abort."""


def validate_key(raw: str) -> str:
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _hash_fd(f) -> str:
    """Hash an open file without disturbing its read position."""
    h = hashlib.sha256()
    pos = f.tell()
    try:
        f.seek(0)
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    finally:
        f.seek(pos)
    return h.hexdigest()


# --------------------------------------------------------------------- #
# version registry                                                       #
# --------------------------------------------------------------------- #


class Version:
    """Immutable metadata of one on-disk package version (one inode)."""

    __slots__ = ("key", "ino", "size", "sha256")

    def __init__(self, key: str, ino: tuple[int, int], size: int, sha256: str) -> None:
        self.key = key
        self.ino = ino
        self.size = size
        self.sha256 = sha256


class VersionRegistry:
    """Tracks the current version per key and every version still downloaded.

    ``sha_by_ino`` holds the digest of every live inode: the current version
    of each key plus superseded versions that still have open downloads. A
    superseded inode's entry is dropped when its last download closes; the
    inode's disk space is reclaimed by the kernel (``os.replace`` already
    unlinked its path). All mutations go through ``lock``; critical sections
    are syscall-fast and never await.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.current: dict[str, Version] = {}
        self.sha_by_ino: dict[tuple[int, int], str] = {}
        self.open_downloads: dict[tuple[int, int], int] = {}

    def _retire_locked(self, ino: tuple[int, int]) -> None:
        """Forget a superseded inode once nobody is downloading it."""
        if self.open_downloads.get(ino, 0) == 0:
            self.sha_by_ino.pop(ino, None)

    def publish(self, key: str, ino: tuple[int, int], size: int, sha256: str) -> None:
        """Register an already-in-place file as the current version of key."""
        with self.lock:
            old = self.current.get(key)
            self.current[key] = Version(key, ino, size, sha256)
            self.sha_by_ino[ino] = sha256
            if old is not None and old.ino != ino:
                self._retire_locked(old.ino)

    def replace_and_publish(
        self, key: str, tmp: Path, final: Path, ino: tuple[int, int], size: int, sha256: str
    ) -> None:
        """Atomically swap ``tmp`` into ``final`` and publish it as current.

        The digest is registered before the rename so that any GET which can
        open the new file is guaranteed to find its version metadata.
        """
        with self.lock:
            self.sha_by_ino[ino] = sha256
            old = self.current.get(key)
            os.replace(tmp, final)
            self.current[key] = Version(key, ino, size, sha256)
            if old is not None and old.ino != ino:
                self._retire_locked(old.ino)

    def acquire(self, key: str) -> "Download | None":
        """Open the current package file and pin its version for one download.

        Returns None when the package does not exist. The returned Download
        holds the open fd; its size/digest are guaranteed to match the bytes
        that fd will yield, no matter how often the key is replaced later.
        """
        with self.lock:
            p = PKG_DIR / key
            try:
                if not p.is_file():
                    return None
                f = open(p, "rb")
            except OSError:
                return None
            try:
                st = os.fstat(f.fileno())
                ino = (st.st_dev, st.st_ino)
                sha = self.sha_by_ino.get(ino)
                if sha is None:
                    # File not written via the control API (dropped into
                    # PKG_DIR externally): hash the open fd once, register it.
                    sha = _hash_fd(f)
                    self.sha_by_ino[ino] = sha
                cur = self.current.get(key)
                if cur is None or cur.ino != ino:
                    if cur is not None:
                        self._retire_locked(cur.ino)
                    self.current[key] = Version(key, ino, st.st_size, sha)
                self.open_downloads[ino] = self.open_downloads.get(ino, 0) + 1
                return Download(key, ino, f, st.st_size, sha, self)
            except Exception:
                f.close()
                raise

    def release(self, key: str, ino: tuple[int, int]) -> None:
        """One download finished; forget superseded inodes with no readers."""
        with self.lock:
            n = self.open_downloads.get(ino, 0) - 1
            if n > 0:
                self.open_downloads[ino] = n
                return
            self.open_downloads.pop(ino, None)
            cur = self.current.get(key)
            if cur is None or cur.ino != ino:
                self.sha_by_ino.pop(ino, None)

    def stats(self) -> tuple[dict[str, dict], int]:
        with self.lock:
            packages = {
                key: {"size": v.size, "sha256": v.sha256}
                for key, v in self.current.items()
            }
            return packages, sum(self.open_downloads.values())


registry = VersionRegistry()


class Download:
    """One accepted download: an open file pinned to one package version."""

    __slots__ = ("key", "ino", "file", "size", "sha256", "_registry", "_closed")

    def __init__(self, key, ino, file, size, sha256, registry_ref) -> None:
        self.key = key
        self.ino = ino
        self.file = file
        self.size = size
        self.sha256 = sha256
        self._registry = registry_ref
        self._closed = False

    def close(self) -> None:
        """Idempotently release the fd and the registry slot."""
        if self._closed:
            return
        self._closed = True
        try:
            self.file.close()
        finally:
            self._registry.release(self.key, self.ino)


def _regenerate(key: str, size: int) -> tuple[int, str]:
    """Write a fresh random package and atomically publish it as current."""
    p = PKG_DIR / key
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f"{p.name}.tmp-{uuid.uuid4().hex[:12]}")
    h = hashlib.sha256()
    try:
        with open(tmp, "wb") as f:
            left = size
            while left > 0:
                b = os.urandom(min(1024 * 1024, left))
                f.write(b)
                h.update(b)
                left -= len(b)
        st = os.stat(tmp)
        registry.replace_and_publish(
            key, tmp, p, (st.st_dev, st.st_ino), st.st_size, h.hexdigest()
        )
    finally:
        _unlink_quiet(tmp)  # no-op once the rename succeeded
    return st.st_size, h.hexdigest()


def _hash_file(key: str) -> None:
    """Adopt an existing on-disk package (boot) as the current version."""
    p = PKG_DIR / key
    st = p.stat()
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    registry.publish(key, (st.st_dev, st.st_ino), st.st_size, h.hexdigest())


@asynccontextmanager
async def lifespan(app: FastAPI):
    PKG_DIR.mkdir(parents=True, exist_ok=True)
    # Drop temp files left by a replace that crashed mid-write.
    for stray in PKG_DIR.rglob("*.tmp-*"):
        if re.fullmatch(r".*\.tmp-[0-9a-f]{12}", stray.name):
            _unlink_quiet(stray)
    for name, size in DEFAULT_PACKAGES.items():
        if (PKG_DIR / name).is_file():
            await asyncio.to_thread(_hash_file, name)
        else:
            await asyncio.to_thread(_regenerate, name, size)
    yield


app = FastAPI(title="pkg-origin", version="1.1.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


# --------------------------------------------------------------------- #
# package download                                                       #
# --------------------------------------------------------------------- #


async def _wait_disconnect(receive) -> None:
    """Return once the ASGI server reports the client is gone."""
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            return


class PackageResponse(Response):
    """Streams one pinned Download and always releases it exactly once.

    The release lives in a ``finally`` around the whole ASGI cycle, so the
    file and its registry slot are freed on completion, on the upstream-abort
    fault injection, and on client disconnect — whether the client leaves
    before the first chunk or mid-body (a disconnect watcher stops the stream
    promptly instead of sending the remaining body into a dead connection).
    """

    def __init__(self, download: Download, beh: dict) -> None:
        self.download = download
        self.beh = beh
        self.status_code = 200
        self.media_type = "application/octet-stream"
        self.background = None
        sha = download.sha256
        if beh.get("send_wrong_sha256"):
            # Fault injection: advertise a digest that does not match the body.
            sha = hashlib.sha256(b"bogus-digest").hexdigest()
        self.init_headers(
            {
                "Content-Length": str(download.size),
                "X-Package-Sha256": sha,
            }
        )

    async def __call__(self, scope, receive, send) -> None:
        dl = self.download
        try:
            await send(
                {
                    "type": "http.response.start",
                    "status": self.status_code,
                    "headers": self.raw_headers,
                }
            )
            disconnect = asyncio.ensure_future(_wait_disconnect(receive))
            try:
                sent = 0
                beh = self.beh
                chunk_size = max(1, int(beh["chunk_size"]))
                delay = max(0, int(beh["chunk_delay_ms"])) / 1000.0
                trunc = beh["truncate_after_bytes"]
                while True:
                    if disconnect.done():
                        return  # client is gone; the finally releases the file
                    if delay:
                        await asyncio.sleep(delay)
                    chunk = await asyncio.to_thread(dl.file.read, chunk_size)
                    if not chunk:
                        break
                    if trunc is not None and sent + len(chunk) > trunc:
                        remain = max(0, int(trunc) - sent)
                        if remain:
                            sent += remain
                            await send(
                                {
                                    "type": "http.response.body",
                                    "body": chunk[:remain],
                                    "more_body": True,
                                }
                            )
                        # Simulate upstream abort: die mid-body after a partial
                        # response so the client sees a truncated payload.
                        raise UpstreamAbort("simulated upstream abort")
                    sent += len(chunk)
                    await send(
                        {"type": "http.response.body", "body": chunk, "more_body": True}
                    )
                await send(
                    {"type": "http.response.body", "body": b"", "more_body": False}
                )
            finally:
                disconnect.cancel()
                with suppress(asyncio.CancelledError):
                    await disconnect
        finally:
            dl.close()


@app.get("/packages/{path:path}")
async def get_package(path: str):
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    # Count every fetch attempt against the origin, successful or not.
    hits["total"] += 1
    hits["paths"][key] = hits["paths"].get(key, 0) + 1

    dl = registry.acquire(key)
    if dl is None:
        return PlainTextResponse("package not found", status_code=404)
    try:
        # The version is pinned now: headers and body come from dl's open fd.
        return PackageResponse(dl, dict(behavior))
    except Exception:
        dl.close()
        raise


# --------------------------------------------------------------------- #
# control API                                                            #
# --------------------------------------------------------------------- #


class Behavior(BaseModel):
    chunk_delay_ms: int = Field(0, ge=0, description="sleep per streamed chunk")
    truncate_after_bytes: int | None = Field(
        None, ge=0, description="abort the stream after this many bytes"
    )
    chunk_size: int = Field(256 * 1024, ge=1, description="origin stream chunk size")
    send_wrong_sha256: bool = Field(
        False, description="fault injection: advertise a digest that != body"
    )


@app.post("/control/behavior")
async def set_behavior(b: Behavior):
    """Set delay / truncation / digest spoofing for subsequent downloads."""
    behavior.update(b.model_dump())
    return behavior


@app.get("/control/behavior")
async def get_behavior():
    return behavior


content_lock = asyncio.Lock()


@app.post("/control/content/{path:path}")
async def change_content(path: str, size: int | None = None):
    """Replace the package with fresh random content (new sha256), atomically.

    Downloads already accepted keep reading the previous version to the end;
    GETs accepted after this call returns serve the new version.
    """
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    async with content_lock:
        p = PKG_DIR / key
        if size is None:
            size = p.stat().st_size if p.is_file() else 32 * 1024 * 1024
        if size <= 0:
            return PlainTextResponse("size must be positive", status_code=400)
        new_size, sha = await asyncio.to_thread(_regenerate, key, size)
    return {"path": key, "size": new_size, "sha256": sha}


@app.get("/control/stats")
async def stats():
    """Origin fetch counters, current package digests, open downloads."""
    packages, open_downloads = registry.stats()
    return {
        "total_hits": hits["total"],
        "paths": dict(hits["paths"]),
        "packages": packages,
        "open_downloads": open_downloads,
        "behavior": dict(behavior),
    }


@app.post("/control/reset")
async def reset():
    hits["total"] = 0
    hits["paths"] = {}
    return {"ok": True}
