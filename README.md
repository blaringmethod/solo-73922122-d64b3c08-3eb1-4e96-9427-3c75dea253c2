# 检验附件入仓桥（Lab Inbox Bridge）

共享邮箱 → 入仓作业的采集服务。通过 IMAP 读取 INBOX，把每封邮件中 1～3 个
UTF-8 CSV 检验附件（列：`报告号,样本号,项目,测值`，每件 ≤ 256 KiB，每件仅一个
报告号）持久化为不可变原件与回执，然后才把邮件标记为已读。网页与 API 可发起
采集、重试标记、查看拒收原因与“待标记”状态。

同一套网页还提供**齐套核对**：上传批号 + 至多 100 行 `样本号,项目` 清单，用
已收讫报告的全部数据行判断样本能否交分析。既有报告在线补建索引，补建期间的
清单只显示“准备中”；补建完成后逐对报告缺项、冲突与来源报告号，冲突或收到
异值时立即取消“可交分析”。

```
┌──────────┐   IMAP    ┌────────────┐   SQLite + 文件卷   ┌────────────┐
│ 共享邮箱  │◀─────────▶│ web-a      │◀──────────────────▶│ /data      │
│ INBOX    │           │ web-b      │                     │ store/     │
└──────────┘           └────────────┘◀───────────────────▶└────────────┘
                             ▲ 网页 / API（两个实例同库同卷）
```

## 快速开始

### Docker Compose（真实双实例 + 本地 IMAP 演示源）

```bash
docker compose up -d --build        # 需要本机 Docker daemon
./scripts/demo.sh                   # 逐项演示下列保证
# web-a http://localhost:8000   web-b http://localhost:8001
# 演示 IMAP localhost:1143       演示控制台 http://localhost:8081
```

Compose 启动三个容器：`demo-imap`（本地 IMAP 演示源 + 控制 API）、`web-a` 与
`web-b`（同一镜像、两个实例，共享 `shared-data` 卷中的 SQLite 与原件存储）。
两者都通过 `depends_on: service_healthy` 等到 IMAP 就绪。

### 本地（无 Docker）

```bash
pip install -r requirements.txt
python -m imap_demo.server --imap-port 1143 --control-port 8081 --data /tmp/demo-data &
INSTANCE_ID=web-a DATABASE_PATH=/tmp/bridge.sqlite3 STORAGE_DIR=/tmp/store \
IMAP_PORT=1143 IMAP_ACCOUNT=lab@demo AUTO_SCAN_INTERVAL=0 \
  python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

`APP_A`/`APP_B`/`DEMO` 环境变量可让 `scripts/demo.sh` 指向任意地址。齐套
核对页面：`/checklists`（`web-a` 与 `web-b` 读写同一个库，结论一致）。

## 交付保证与实现位置

| 需求 | 实现 |
| --- | --- |
| 只采未读会漏收 → 采集全部 UID | `Collector._list_uids` 固定 `UID SEARCH ALL`；人工已读的邮件照常入库（`tests/test_collector.py::test_pre_read_message_is_collected`） |
| 附件格式校验：UTF-8、≤256 KiB、1～3 件、每件一个报告号 | `app/mailparse.py`；表头、列数、空字段、重复报告号逐项校验 |
| 格式错误或同号异文 → 整封拒收，不留部分新报告 | 先整封校验得到 plans，再在**单个 SQLite 事务**里做冲突判定与写入；冲突时回滚并写拒收记录（`_commit_acceptance_tx` / `_Conflict`） |
| 同号同原件复用 | 以附件 SHA-256 判定；相同则 `delivery.disposition='reused'`，报告只保留一份（`report` 表唯一键 `account+mailbox+report_no`） |
| 全部附件与回执持久化后才标已读 | `Store.put_original/put_attachment/put_receipt` 均 `fsync` + 原子 rename；`_commit_acceptance` 提交后才进入 `_mark_pending` |
| 标记已生效却断连 → 不重复报告 | 标记前重新 FETCH 并比对 `raw_sha256`；IMAP 连接中断时扫描记为 `interrupted`（已提交的不回退），该行保持 `pending`，`POST /api/retry-marks` 或下次扫描补标；重新扫描按 `(account,mailbox,uidvalidity,uid)` 去重 |
| 入库后退出 / 重启 | 状态全在 SQLite + 文件卷；新进程续扫，报告幂等 |
| UIDVALIDITY 变化后重扫 | `claim_scan`/`adopt_changed_uidvalidity` 提升 `generation`、清空 `last_uid` 与租约，旧代 `pending` 标记转 `stale` |
| 复用 UID 不套旧回执 | 主键含 `uidvalidity`；标记前比对 UID 当前内容的 SHA-256，不一致则拒绝标记并转 `stale` |
| 两实例并发 / 旧扫描不推进新进度 | `mailbox_state` 中的扫描租约（token + 心跳）；`set_progress`、`finish_scan`、标记全部以 token 与 uidvalidity 为条件 |
| 标记后断连可演示 | 演示源 `POST /ctl/drop-store`：STORE 已生效但连接在回复前被关闭 |
| 网页：采集 / 重试 / 拒收原因 / 待标记 | `GET /`、`POST /actions/scan`、`POST /actions/retry-marks`、`?mark=pending` 过滤；API 见下 |
| 齐套核对：批号 + ≤100 行清单，受理后不可改 | `POST /api/checklists`（multipart `file` + `batch_no`）；`checklist_pairs` 的 `UNIQUE(checklist_id, sample_no, item)` 与 `checklist` 的 `UNIQUE(batch_no)`；解析见 `app/checklist.py::parse_checklist` |
| 用已收讫报告的全部数据行核对；同对同测值只计一次；异值列出冲突报告号 | `report_value` / `report_value_source` 投影按 `(sample_no,item,value)` 去重；`checklist_detail.html` 按值分组列出来源报告号；状态由“不同测值个数”决定（0 缺项 / 1 齐套 / ≥2 冲突），测值按字符串比较 |
| 无缺项且无冲突才报齐套 | `checklist.state='ready'` 仅当全部 pair 为 `ok`；页面文案“可交分析”只在此时出现 |
| 既有报告在线补建、持续增量维护、查询不重扫报告库 | 补建游标 `backfill_state.cursor_report_id` 分批写入投影；页面/API 只读投影与索引；新报告在同一 SQLite 事务里写投影（`record_live_values_tx`） |
| 补建期间仍可采集，已有及同期新增报告不得漏计或重复计入 | 补建只持有短批次写锁并让出；入库事务与补建对同一唯一键 `INSERT OR IGNORE`，两侧都写不会重复；完成后再补一轮即可覆盖补建期间落库、且入库事务已写入的报告 |
| 完成前仅报准备中 | `checklist.state='evaluating'`；API 返回 `state="preparing"` 且 `pairs=[]`；补建失败/中断保持 `evaluating` |
| 齐套后收到相关异值，随采集入库一起转冲突 | `_commit_acceptance_tx` 在同一事务里调用 `evaluate_open_checklist_tx`，冲突与报告同时提交，页面不再显示可交分析 |
| 拒收邮件不参与 | 只有 `_commit_acceptance_tx` 写投影；拒收路径不写 `report`/`report_value` |
| 双实例不重复累计、重启接续、失败不开放半份结论 | 补建租约 `backfill_state.lease_token` + 每批条件更新；提交点即游标，重启从游标续跑；`try_finish_backfill` 与清单初始化同事务 |
| 网页可查缺项、冲突及来源 | `/checklists`、`/checklists/{id}`；来源含报告号、采集/补建标记、消息与附件原件链接 |
| 保留原采集和下载行为 | 仪表盘与既有 `/api/messages`、原件/回执/附件下载未改；`tests/test_web.py` 全绿 |

### 关键不变式

1. **先落盘后标记**：文件 fsync + SQLite 提交 → 才可能发出 `STORE +FLAGS \Seen`。
2. **整封原子性**：一封邮件要么全部附件入仓，要么一条报告都不新增。
3. **消息身份 = (account, mailbox, uidvalidity, uid)**；重试与重启只更新同一条记录。
4. **旧扫描无写权限**：所有写操作都带扫描 token 与 UIDVALIDITY 条件；检测到重建即中止，且不标记、不推进 `last_uid`。
5. **标记前内容校验**：UID 当前原文 SHA-256 必须等于入库时的 SHA-256。

## 存储布局

```
/data/bridge.sqlite3          SQLite（WAL；含核对投影与补建游标）
/data/store/msgs/<sha256>.eml
/data/store/attachments/<报告号>/<sha256>-<文件名>.csv
/data/store/receipts/<sha256>.json
```

原件与回执均为内容寻址，重复写入天然幂等；回执包含 UID、UIDVALIDITY、原文
SHA-256、附件 SHA-256 与全部数据行，可脱离数据库核对。

## 网页 / JSON API

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| GET | `/` | 仪表盘：统计、邮件记录（收讫/拒收原因/标记状态）、入库报告、扫描历史 |
| GET | `/?mark=pending` | 只看待标记 |
| POST | `/actions/scan` | 页面“立即采集” |
| POST | `/actions/retry-marks` | 页面“重试待标记” |
| GET | `/api/status` | 实例、IMAP 探测、统计、邮箱进度、当前租约 |
| POST | `/api/scan` | 采集；另一实例持有租约时返回 409 |
| POST | `/api/retry-marks` | 只补标记，不扫描新邮件 |
| GET | `/api/messages?mark_state=` | 邮件记录（含 `detail_list` 拒收原因、`reports`） |
| GET | `/api/messages/{id}/original` | 下载原始 RFC822 原件 |
| GET | `/api/messages/{id}/receipt` | 下载固定回执 JSON |
| GET | `/api/reports/{id}/attachment` | 下载 CSV 原件 |
| GET | `/checklists` | 齐套核对：补建状态、提交清单、已受理清单 |
| POST | `/checklists` | 页面提交清单（multipart `file` + `batch_no`），303 到详情 |
| GET | `/checklists/{id}` | 逐对缺项 / 冲突 / 来源 |
| POST | `/checklists/backfill` | 页面按钮：把补建推进到完成 |
| POST | `/api/checklists` | 受理清单；成功 201，格式错误 400，重复批号 409 |
| GET | `/api/checklists` | 清单列表与计数 |
| GET | `/api/checklists/{id}` | 清单结论；补建中 `state=preparing`、`pairs=[]` |
| POST | `/api/checklists/backfill` | 推进补建（可重复调用） |
| GET | `/api/backfill` | 补建状态、游标、百分比 |

演示源控制 API（`demo-imap:8081`）：`POST /ctl/deliver` 投递、`POST /ctl/rebuild`
（`{"purge":false}` 重建 UIDVALIDITY 并重排 UID）、`POST /ctl/drop-store`
（下一次 STORE 生效后断连）、`GET /api/state`。

## 测试

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

覆盖：CSV 校验矩阵、整封拒收、同号同原件复用、同号异文冲突、预先已读、
标记后断连 + 补标记、UIDVALIDITY 重建与 UID 复用、扫描中途重建、并发租约、
重启续扫、下载接口与网页动作；以及齐套核对的清单解析矩阵（≤100 行、重复对、
批号一致）、补建增量/续跑/双实例并发不重复、补建中只报准备中、缺项/同值去重/
异值冲突、齐套后晚到异值翻转、拒收不参与、清单不可改。

## 配置项

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `INSTANCE_ID` | `instance` | 实例标识（租约/日志） |
| `DATABASE_PATH` | `/data/bridge.sqlite3` | 共享 SQLite 路径 |
| `STORAGE_DIR` | `/data/store` | 原件/附件/回执根目录 |
| `IMAP_HOST` / `IMAP_PORT` | `127.0.0.1` / `1143` | IMAP 地址 |
| `IMAP_ACCOUNT` | `lab@imap-demo` | 数据库中的逻辑账号键 |
| `IMAP_USER` / `IMAP_PASSWORD` | `lab` / `lab-pass` | 登录凭据 |
| `IMAP_MAILBOX` | `INBOX` | 采集文件夹 |
| `MAX_ATTACHMENT_BYTES` | `262144` | 单附件上限 |
| `MAX_ATTACHMENTS_PER_MAIL` | `3` | 单封附件数上限 |
| `AUTO_SCAN_INTERVAL` | `15`（Compose 中 `0`） | 自动扫描间隔秒；0 关闭 |
| `SCAN_LEASE_SECONDS` | `60` | 扫描租约时长 |
| `MAX_CHECKLIST_BYTES` | `65536` | 清单 CSV 上限 |
| `AUTO_BACKFILL` | `1` | 启动后台补建循环 |
| `BACKFILL_BATCH_SIZE` | `200` | 每批报告数 |
| `BACKFILL_BATCHES_PER_TICK` | `5` | 每次循环批数（有界写锁） |
| `BACKFILL_PAUSE_SECONDS` | `0.05` | 批次之间让出时间 |
| `BACKFILL_LEASE_SECONDS` | `60` | 补建租约时长 |
| `BACKFILL_RETRY_SECONDS` | `5` | 租约被占用时的重试间隔 |

## 未实现 / 边界

* 生产邮箱的 TLS（`IMAPS`）与 OAuth 未接入；当前用明文 `IMAP4`，演示源亦为明文。
* 无用户认证/授权；不要把端口直接暴露到不可信网络。
* 拒收邮件按“已处理”标记 `\Seen`，不移动到隔离文件夹；重投/人工处理需在邮箱侧重发。
* 未实现附件级杀毒、压缩包展开、非 UTF-8 转码、超过 3 件邮件的拆分。
* SQLite 适合两实例共享卷；更大规模应换 PostgreSQL 与对象存储（租约与幂等键设计可直接迁移）。
* 演示 IMAP 源仅实现采集所需子集（无 TLS、无 ACL、无 IDLE）；不可用于生产。
