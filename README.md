# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 API + `/docs` 清除入口）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status
```

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数、当前包 sha256 与在途下载数
curl http://localhost:8081/control/stats

# 更换包内容（原子替换：已受理的下载读完旧版，新 GET 读新版）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144,"send_wrong_sha256":false}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536,"send_wrong_sha256":false}'

# 发送与正文不符的 X-Package-Sha256（故障注入，验证代理拒收）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":null,"chunk_size":262144,"send_wrong_sha256":true}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 行为要点

* **源站版本一致**：换包先写临时文件并登记新 inode 的摘要，再 `os.replace`
  原子生效。每次 GET 在受理时即打开包文件，`Content-Length` 取自该打开
  文件的 `fstat`、`X-Package-Sha256` 取自按 inode 登记的摘要——同一次
  响应的正文、长度、摘要必属同一版。换包成功后的新 GET 读新版；已受理
  的旧下载凭已打开的 fd 读完旧版（内核为已删除的 inode 保活到最后一个
  fd 关闭）。下载结束、出错或客户端断开（首块前或中途）时，本次下载持有
  的 fd 与版本登记由响应 `finally` 恰好释放一次（`/control/stats` 的
  `open_downloads` 可观察）。
* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。代理填充时
  边下边算 sha256，若收到包的长度或摘要与正文不符（`X-Package-Sha256`
  校验失败），同样判失败、不缓存；已知包长的最后一块会扣留到填充校验
  通过后才发给客户端，保证末尾才发现的失败对客户端表现为截断而非假成功。
* **断开释放**：代理每个响应持有一个读者名额，由包裹整个 ASGI 周期的
  `finally` 恰好释放一次；断开监听让流在客户端离开时立即停止。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` 残件与孤儿
  meta。404 不做负缓存。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越，以及下载/换包交错（源站与代理两侧的一致性、并发换包下
每个响应自洽）、客户端断开（首块前/中途）释放文件资源、摘要与正文不符
时代理拒收不缓存。`tests/run_server.py` 是测试专用启动器（本沙箱禁止
进程间信号，用控制文件代替 SIGTERM 停服），交付应用本身不含测试钩子。
