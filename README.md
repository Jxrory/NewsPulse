# NewsPulse

个人自托管的新闻简报服务：定时采集 RSS / Atom，去重入库，用可配置的 chat_completions 模型筛选与撰写中文简报，并通过飞书群自定义机器人 Webhook 发送富文本消息。

设计依据与需求规格在 [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md)；数据库草案在 [`docs/schema-proposal.sql`](docs/schema-proposal.sql)；飞书协议核实记录在 [`docs/research-feishu-push.md`](docs/research-feishu-push.md)。

## 已实现（v1）

- 单 Go 进程，无 Redis / PostgreSQL / n8n / 向量库依赖。
- 每年一个 SQLite 文件（`news-YYYY.sqlite`），按**首次采集时刻**的年度归属；跨年重复观测写回原库。
- RSS/Atom/RDF 解析、GBK 等编码处理、HTTP ETag/Last-Modified 缓存、失败退避、源健康记录。
- 确定性新闻去重：规范化 URL、同 feed GUID、弱内容指纹；所有年度库全局 alias 查找；冲突不静默合并。
- 采集观测、正文修订、跨年候选查询、来源状态。
- 规则候选排序、同语言标题相似度事件分组、可配置模型分析、严格 JSON/引用校验、失败降级为 RSS 摘要。
- 每日预算预留（跨年度库汇总）、模型调用记录与成功结果缓存。
- 持久化 digest jobs、调度槽唯一键、补跑窗口、重复事件抑制。
- digest + items + outbox 在同一年度库同一事务保存。
- 飞书 `post` 富文本渲染、签名、最终请求体大小校验、`sent`/`retry_wait`/`failed`/`send_unknown`/`expired` 状态机、崩溃后 `sending` 转 `send_unknown`。
- CLI 与本机 HTTP 管理接口、备份（`VACUUM INTO` + manifest + 校验）、systemd 示例。
- 单元/集成测试覆盖去重、跨年归属、checkpoint、digest/outbox 原子性、签名、渲染长度、配置、调度槽。

## 未实现 / 明确限制

- **全文抓取未实现**（`editorial.include_full_article_fetch` 必须为 false）；只用 RSS 提供的标题/摘要/正文文本。
- 消息自动拆分未实现（`notification.split_long_messages` 必须为 false）；超长消息按固定顺序精简，完整版保存在本地。
- 异地备份复制未实现（`backup.remote_copy_enabled` 仅告警）。
- Lark 国际版端点未支持；只允许 `https://open.feishu.cn/open-apis/bot/v2/hook/...`。
- 不承诺新闻全覆盖、模型事实判断绝对正确、飞书端到端不重复或群消息永久保存。
- 当前 SQLite 驱动为 `github.com/mattn/go-sqlite3`，构建需要 CGO 与 C 编译器。设计稿原建议 CGO-free 的 `modernc.org/sqlite`；如需纯静态交叉编译，可替换驱动后重新跑全部测试（SQL 语义不变）。

## 构建

```bash
go build -o newspulse ./cmd/newspulse
# 或
go install ./cmd/newspulse
```

要求 Go 1.26+（依赖模块的最低版本），以及 CGO 工具链（macOS 的 clang / Linux 的 gcc）。

## 配置与密钥

```bash
cp configs/config.example.yaml configs/config.yaml
mkdir -p /etc/newspulse
cp configs/config.example.yaml /etc/newspulse/config.yaml
```

密钥只从环境变量读取，不写入 YAML、不提交 Git、不输出到日志：

```bash
# /etc/newspulse/newspulse.env (chmod 600)
NEWSPULSE_ADMIN_TOKEN=<随机长 token>
LLM_BASE_URL=https://api.example.com/v1
LLM_API_KEY=<模型供应商 key>
LLM_MODEL=<模型名>
FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/<token>
FEISHU_SIGN_SECRET=<飞书机器人签名密钥>
```

`configs/config.example.yaml` 默认：

- `schedule.enabled: false`、`times: []` — 未确认推送时刻前不会自动生成/发送。
- `notification.enabled: false` — 只会生成草稿。
- `llm.enabled: true` — 但必须填写 `LLM_*` 环境变量，`config validate` 会检查变量存在与取值合法性。

```bash
NEWSPULSE_ADMIN_TOKEN=x LLM_BASE_URL=... LLM_API_KEY=... LLM_MODEL=... \
  ./newspulse config validate --config configs/config.yaml
```

## 常用命令

```bash
# 真实网络源检查（会发起 GET，并抽样打开原文链接）
./newspulse sources check --config /etc/newspulse/config.yaml --links 2

# 手动采集一轮（每个启用源各一次）
./newspulse fetch --once --config /etc/newspulse/config.yaml

# 生成一份草稿（保存本地，不进入推送队列）
./newspulse digest generate --config /etc/newspulse/config.yaml --no-fetch
./newspulse digest generate --config /etc/newspulse/config.yaml --json --at 2026-11-24T07:15:00+08:00

# 查看历史简报
./newspulse digest show --config /etc/newspulse/config.yaml --format markdown 2026/<id>

# 手动入队 / 查看 / 显式重发（不会重新生成新闻内容）
./newspulse notify enqueue --config /etc/newspulse/config.yaml --digest 2026/<id>
./newspulse notify status  --config /etc/newspulse/config.yaml --digest 2026/<id>
./newspulse notify resend  --config /etc/newspulse/config.yaml --digest 2026/<id> --reason "manual retry"

# 维护
./newspulse db migrate --config /etc/newspulse/config.yaml --all
./newspulse db verify  --config /etc/newspulse/config.yaml --all
./newspulse backup create --config /etc/newspulse/config.yaml
```

`digest generate` 永远只生成草稿；真实发送只能通过调度开启后的自动 outbox，或显式 `notify enqueue/resend`。

## 调度与飞书联调

1. 在目标飞书群添加“自定义机器人”，复制 Webhook，开启签名校验，将 Webhook 与密钥写入服务器环境文件。
2. 若机器人开启关键词/IP 限制，在 `notification.required_keyword` 填关键词（模板会固定包含），并把 VPS 出口 IP 加入白名单。
3. 先保持 `schedule.enabled: false`，用 `digest generate` 检查中文、分节与来源链接。
4. 显式 `notify enqueue` 做一次真实验收；确认群内可读、链接正确、手机端显示正常。
5. 最后再确认推送时间并设置 `schedule.times`，例如 `["07:15"]`（这只是一个示例，不代表本项目替你选定了时间）。时间未确认前保持 `schedule.enabled: false`。

签名算法固定为：

```text
key = timestamp + "\n" + secret
HMAC-SHA256(key, empty) -> 标准 Base64
```

只把 `HTTP 2xx + 明确整数 code=0` 视为 `sent`。超时、断连、5xx、缺 code 等无法判断是否已送达的情况标记 `send_unknown`，默认不自动重发；核对群消息后使用 `notify resend`。

## HTTP 管理接口

默认 `127.0.0.1:8080`，健康检查不需要鉴权，其余接口需要 `Authorization: Bearer $NEWSPULSE_ADMIN_TOKEN`。

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/healthz` | 进程存活 |
| GET | `/readyz` | 存储/迁移/必需配置状态；`mode=collection_only` 表示调度关闭 |
| GET | `/api/v1/status` | 源健康、最近简报、outbox 计数、LLM 当日预算 |
| GET | `/api/v1/articles` | `from`/`to`(RFC3339)、`section`、`source_id`、`publisher_id`、`limit`、`before_published`+`before_id` 游标 |
| GET | `/api/v1/digests` | 年份+ID 游标分页 |
| GET | `/api/v1/digests/{year}/{id}` | JSON；加 `?format=markdown` 返回 Markdown |
| POST | `/api/v1/jobs/fetch` | 202；创建采集任务 |
| POST | `/api/v1/jobs/digest` | 202；创建草稿生成任务，可选 `{"at":"RFC3339","fetch":true}` |
| POST | `/api/v1/digests/{year}/{id}/notify` | 幂等入队，返回已有或新建 outbox |

列表查询跨全部年度库按 `published_at DESC, id DESC` 合并排序，再计算下一页。

## 命令并发与单写者

数据目录由 `newspulse.lock` 保证单写者。daemon 运行时再次执行写库命令不会直接失败：

- `fetch`、`digest generate`、`notify enqueue` 检测到 daemon 持有锁后，自动通过本地 HTTP API 提交等价任务（需要 `NEWSPULSE_ADMIN_TOKEN`，并读取 `http.listen`）。
- `notify resend` 涉及人工确认，必须停止 daemon 后执行；`db migrate`、`backup create`、`db verify` 同样建议停机执行。
- 如果锁被另一个 CLI 进程占用且 API 不可达，命令会返回明确错误，不会绕过锁。

## 数据目录与备份

```text
/var/lib/newspulse/
  newspulse.lock
  db/news-2026.sqlite
  db/news-2027.sqlite
  backups/backup-YYYYMMDDTHHMMSSZ/
    news-2026.sqlite
    manifest.json
```

- 备份使用 `VACUUM INTO` 生成一致快照，包含 WAL 内容；不会只复制主文件。
- 备份期间暂停新的采集/生成/状态写入；进行中的外部发送结果在恢复时会转为 `send_unknown`，避免假定“没发过”。
- 恢复流程：停止服务 → 用备份文件替换 `db/` 下对应年份 → 启动后先确认已发送消息 → 再恢复发送。旧备份可能包含实际已发出但状态未落库的 outbox，**恢复后默认暂停发送 worker**，核对后再处理。
- 不默认删除历史年度库；`fetch_runs`、缓存等可按需另行清理。

## 部署（systemd）

```bash
sudo useradd --system --create-home --home-dir /var/lib/newspulse newspulse
sudo mkdir -p /opt/newspulse/bin /etc/newspulse /var/lib/newspulse
sudo cp newspulse /opt/newspulse/bin/
sudo cp configs/config.example.yaml /etc/newspulse/config.yaml
sudo cp deploy/systemd/newspulse.service /etc/systemd/system/newspulse.service
sudo install -m 600 -o newspulse -g newspulse secrets.env /etc/newspulse/newspulse.env
sudo systemctl daemon-reload
sudo systemctl enable --now newspulse
journalctl -u newspulse -f
```

服务日志为 JSON，敏感字段（Webhook 路径 token、签名、API key、Authorization）会被脱敏；仍建议限制日志访问权限。

## Docker 部署

镜像从源码多阶段构建，运行层为 `scratch`，二进制静态链接 SQLite（CGO + musl），镜像内没有 shell。`vendor/` 不提交，构建阶段通过网络下载 Go 模块；受限网络可换 Go 模块镜像。

```bash
cd deploy/docker
cp config.example.yaml config.yaml
cp newspulse.env.example newspulse.env
chmod 600 newspulse.env
# 编辑 config.yaml（保持 llm.enabled/schedule.enabled/notification.enabled 的既定状态）
# 编辑 newspulse.env：至少填写 NEWSPULSE_ADMIN_TOKEN、LLM_BASE_URL、LLM_API_KEY、LLM_MODEL
docker build --no-cache --network=host --build-arg GOPROXY=https://goproxy.cn,direct -t newspulse:0.1.0 -f Dockerfile ../..
docker compose up -d
curl -sS http://127.0.0.1:8080/healthz
docker compose logs -f
```

- 默认端口只发布到 `127.0.0.1:8080`；需要远程访问时改为受保护的端口映射或前置反向代理。
- 数据保存在命名卷 `newspulse-data`（`/var/lib/newspulse`）；备份可用 `docker cp newspulse:/var/lib/newspulse/backups ./backups` 导出。
- 镜像无 shell，CLI 通过 `docker compose exec newspulse /newspulse ...` 执行；daemon 运行时写命令会自动走本地 API。
- 修改 `config.yaml` 或 `newspulse.env` 后执行 `docker compose up -d --force-recreate`。
- `docker build` 使用 `--network=host` 规避 Docker Desktop BuildKit 的 DNS 解析不稳定；Compose 只负责运行已有镜像，运行阶段仍是隔离网络。
- 在仓库根目录构建时可用：`docker build --no-cache --network=host --build-arg GOPROXY=https://goproxy.cn,direct -t newspulse:0.1.0 -f deploy/docker/Dockerfile .`。

## 测试

```bash
go test ./...
go test -race ./...
go vet ./...
```

测试使用本地 RSS fixture、`httptest.Server`、临时 SQLite 文件与假时钟；不会在 CI 中真实发送飞书消息或调用付费模型。

## 上线前仍需人工确认

1. 具体 VPS、数据目录权限与 systemd 实际部署。
2. 真实 LLM endpoint/model/key/每日预算（含价格估算配置）。
3. 目标飞书群、Webhook、签名密钥与群内机器人权限；完成一次真机接收验收。
4. 推送时区、每天时刻与是否多次推送；在此之前保持调度关闭。
5. 是否只接收群消息；如需要机器人私聊个人，需另行接入飞书应用机器人，不属于当前实现。
