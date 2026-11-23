# 脚本型 cron 失败自动建卡 + 分级升级机制设计（cron-selfheal）

- 编号：OPC-OS 整改 0829-SELFHEAL（Gavin 0829 指令 · 自愈能力补强）
- 作者：昭阳（arch，第 9 人）｜日期：2026-08-29
- 卡：内部卡号略
- 完整指令：(internal msg file, omitted)
- 红线：不碰与本指令无关的改动；不改 Hermes 本体源码；巡检脚本自身健壮（自身失败不 cascade）

## 〇、结论先行（TL;DR）

1. **Gavin「复用 opc-system-monitor 失败→建卡」不存在**——system-monitor.py 只有告警+自救（过热/内存降载、清 tmp、截日志、heal_restore 可逆），全仓 grep 无「失败→建卡」逻辑。可复用的是它的**巡检模式（cron 周期扫描）**和**总线接入模式（_bus_append 失败静默）**，建卡逻辑需新写。
2. **告警总线路由 cron 存活**：make profile `opc-alert-router`（id 976d47b189ca）enabled=true、last_status=ok、last_run_at 08-29 08:59、deliver=local、monitor hash 稳定（e3b0c…=空摘要）。链路本身活着。今天两个故障没触发自愈的**断链点在脚本层**：mrd-host-daily-audit / four-platform-monitor 是业务类 watchdog（08-27 定稿明确「不在本次范围」），**从未接入告警总线**，stdout 直发 telegram——总线路由 agent 根本看不到它们。
3. **新机制定位**：no_agent 巡检脚本 `cron-selfheal-monitor.py`（30min），**不依赖任何监控脚本主动上报**——直接扫全 profile 的 cron jobs.json（Hermes cron 引擎自带的 last_status/failure_streak 就是天然失败信号源），失败即建卡。同时对告警总线做**陈旧回退路由**（age>60min 未路由的 P0/P1 条目由巡检兜底建卡，激活 08-27 定稿的 bus→建卡闭环，不依赖 LLM 路由 agent 一定醒来）。
4. **建卡选型：`hermes kanban create` CLI（官方操作面），不直连 kanban.db 写入**。唯一例外：P0 升级改 title/priority（官方 CLI 无此能力）→ 直连 UPDATE + task_events(kind='edited') 留痕（元数据变更，非状态机迁移）。
5. **幂等**：idempotency_key=`fail-<job_id>-<error_hash>`；同一 job 同一错误只开一张卡；错误变化才开新卡；已存在卡仅按 streak 增量追加评论。**回归场景**（内部卡号略）：内核 idempotency 会命中 done 卡 → 键加确定性后缀 `-r<completed_at>` 开新卡。
6. **分级升级**：failure_streak≥3 → 卡 title 前缀升 【P0 运维】+ priority 200 顶格 + 总线 append P0 事件（mark_routed 立即回写，防路由 agent 双开卡）+ 卡 comment 留痕。P0 事件进当日总线 → 18:50 路由 agent 每日汇总 → 19:00 日报 → 庄子主循环可见（Gavin 知情路径沿用既有 G 卡机制，本机制不新开直推通道）。

## 一、现状盘点（事实，含证据）

| 资产 | 位置 | 事实 | 证据 |
|---|---|---|---|
| opc-alert-bus.py | opc-os/scripts/ + ~/.hermes/scripts/（IDENTICAL） | 统一告警总线，schema 9 字段，CLI append/read_new/mark_routed/mark_resolved | cmp 双副本一致；08-27 定稿 |
| alerts_bus_monitor.py | 同上 | 路由 agent 的 monitor_script（5min tick，hash 静默，[STALE>2h] 自监控） | make profile jobs.json monitor_script 字段 |
| opc-alert-router cron | make profile，id 976d47b189ca | **存活**：enabled=true / last_status=ok / 08:59 刚跑 / deliver=local / monitor hash e3b0c…（空摘要=无未处理告警） | make/cron/jobs.json L463-515 |
| system-monitor.py | opc-os/scripts/ | 告警+自救（降载/清tmp/截日志/heal_restore），_bus_append 失败静默 | L868-883；全仓无建卡逻辑 |
| mrd-host-daily-audit | anran cron 4a375cb37bf8 | **失败**：last_status=error / streak=2 / last_error=行41算术语法错误 | anran/cron/jobs.json L183-225 |
| four-platform-monitor | anran cron e46ffab9aed6 | 失败：gh CLI 环境/代理问题（Gavin 指令 §故障2） | Gavin 指令 + 修复卡 t_364fb671 |
| 两张 P1 修复卡 | kanban | 已存在：t_2eb6f13c（mrd 数字校验加固）/ t_364fb671（gh CLI 环境+降级），anran，running，priority 300 | sqlite 查询 |

**断链点判定**：总线→路由→建卡链路完整存活（router 5min tick 正常）；但 mrd/four-platform 两个业务 watchdog 从未 append 总线（08-27 定稿范围外），stdout 直发 Gavin telegram——「失败→无人兜底」的根因是**监控脚本层与 kanban 之间没有桥**，不是总线坏了。新机制直接扫 jobs.json，绕开「脚本主动上报」这一前置条件，从源头补桥。

## 二、对 Gavin「复用 system-monitor 失败→建卡」的如实评估

**结论：该逻辑不存在，无法复用；可复用的是巡检模式与总线接入模式。**

- 全仓 grep（system-monitor.py）：无 kanban / 建卡 / create_task 任何调用。system-monitor 的「自愈」是**本机动作**（清 /tmp、截日志、停可杀服务、heal_restore 可逆恢复），不是「建卡派活」。
- system-monitor 有告警时 `_bus_append` 总线（P0/P1/P2），然后靠总线→路由 agent 建卡——这条链已经存在且存活。
- 真正缺的是：**不主动 append 总线的脚本（业务 watchdog 等）失败时谁兜底**。Gavin 的直觉方向对（复用监控/巡检体系），但落点是「扫 cron 引擎自身的失败信号」而非「复用 system-monitor 的某个失败→建卡函数」——后者不存在。
- 复用清单（设计继承）：
  - 巡检节奏：no_agent cron 周期扫描（同 system-monitor 5min 模式，本轮 30min）
  - 总线接入：`_bus_append` 失败静默模式（bus 故障不阻塞主流程）
  - env 参数化：OPC_* 全套（同 system-monitor L17-21 模式，HOME=OPC_HOME 默认 <your-home>，cron 派生会话 HOME 被重定向不可用 expanduser）
  - 数据容错：非数字→置 0/跳过；API 异常→缓存旧值；外部 CLI→绝对路径+代理 env；不整崩（本次故障根因，见 §八）

## 三、架构与数据流

```
┌─ 失败信号源（输入面 A）──────────────────────────────┐
│  Hermes cron 引擎：default ~/.hermes/cron/jobs.json  │
│  + 各 profile ~/.hermes/profiles/*/cron/jobs.json    │
│  天然信号：enabled && last_status=error               │
│  （last_error / failure_streak / last_run_at 即证据） │
└──────────────────────┬───────────────────────────────┘
┌─ 告警总线（输入面 B）────────────────────────────────┐
│  opc-alert-bus read_new 的 P0/P1 条目                │
│  仅处理 age>60min 的陈旧条目（路由 agent 正常时       │
│  ≤5min 已路由；60min 未路由 = 路由 agent 挂/漏 = 兜底）│
└──────────────────────┬───────────────────────────────┘
                       ▼
        cron-selfheal-monitor.py（30min，no_agent，anran profile，deliver=local）
        ├─ 失败采集 → 幂等判定（idempotency_key + done 后缀）→ 建卡/评论/升级
        ├─ 建卡：hermes kanban create（assignee=anran，P1/P0，priority 200，
        │        title=【P1|P0 运维】<job名> 失败：<错误摘要>，body=证据+修复提示）
        ├─ 升级（streak≥3）：直连 UPDATE title/priority + task_events(kind='edited')
        │        + 总线 append P0（source=cron-selfheal, type=cron-fail-escalate）
        │        + mark_routed 立即回写（防路由 agent 双开卡）
        ├─ 总线面：陈旧 P0/P1 → 同建卡逻辑 → mark_routed <fingerprint> <card_id>
        └─ 静默契约：无失败/无动作 → stdout 空（no_agent 语义，零 token）
                       ▼
        kanban（anran 运维卡）→ dispatcher 自动派活 → 修复 → done
        ├─ 总线来源条目：卡 done 后由路由 agent/人工 mark_resolved（既有闭环）
        └─ 自愈机制自身：下次巡检不再见该 job 失败 → 不建卡（恢复闭环）
```

## 四、建卡实现选型：官方 CLI，不直连 db（附唯一例外）

| 方案 | 判定 | 理由 |
|---|---|---|
| **`hermes kanban create` CLI** | **采用** | ① tasks+task_events 一致性由内核保证，零孤儿卡风险（红线）；② `--idempotency-key` 原生支持（非 archived 同键返回已有 id——正是幂等语义）；③ `--priority/--assignee/--created-by/--body` 全齐；④ 官方操作面，Hermes 升级 schema 变化不炸（08-27 定稿「跨 profile 改 cron 用 CLI」同款纪律） |
| 直连 kanban.db 写入 | 否决 | 状态机（tasks+task_events+task_runs）是内核所有权；脚本直写=复制状态机语义，Hermes 升级即漂移（唯一事实源原则）。只读查询（幂等键探测）例外：SELECT 无副作用，允许 |
| vendor/kanban-dispatcher CLI | 不适用 | 该组件是**调度器**（claim/spawn），无 create 子命令（cli.py 核对） |

**唯一直接写例外——P0 升级**：`hermes kanban edit` 只支持 result/summary/metadata，官方 CLI 无改 title/priority 能力（CLI 全子命令核对）。升级 = 元数据变更（title 前缀 + priority），非状态机迁移（status/claim/run 不动）。实现：直连 UPDATE tasks SET title/priority + INSERT task_events(kind='edited', payload={field,from,to})（内核 `_append_event` 注释明示 edited 为合法事件类型，run_id=NULL）。事务内完成，日志留痕。文档化此例外，禁止扩大使用面。

## 五、幂等设计（核心）

### 5.1 失败事件键
- `error_hash = sha256(last_error)[:12]`（last_error 为空时用 `"empty"`）
- 基础键 `base_key = fail-<job_id>-<error_hash>`
- **同 job 同错误 → 同键 → 不重复开卡**（内核 idempotency）；**错误变化 → 新 hash → 新键 → 开新卡**（DoD：错误变化才开新卡）

### 5.2 回归场景（内部卡号略）
内核 idempotency 语义：`SELECT id FROM tasks WHERE idempotency_key=? AND status!='archived'`——**done 卡也算命中**，直接返回旧卡 id → 复发不建卡（08-27 定稿已警告此坑）。解法（确定性后缀）：
```
探测 base_key → 命中 done 卡（completed_at=X）→ 键改为 base_key + '-r' + X
  → 再探测 → 若又命中 done（X2）→ 键改为 base_key + '-r' + X2（单调递增，必然收敛）
```
- 确定性：同 tick 复算同键，跨 tick 稳定；X 是 completed_at 常量，不会因重跑变化
- 语义：每轮「修复→复发」周期一张新卡，每张卡一个稳定键

### 5.3 评论节流（幂等命中时的信息更新）
- 幂等命中（卡存在且非 done）→ 不建卡，**仅当 failure_streak 增量变化**（state 文件记录上次值）才追加评论：`【自愈巡检】第 N 次失败 <last_run_at> streak=N`
- streak 未变（同一失败、无新运行）→ 完全静默，防 30min×N 评论刷屏

### 5.4 state 文件
- 路径：`OPC_SELFHEAL_STATE`（默认 `{OPC_ALERT_DIR}/selfheal_state.json`），JSON dict
- 内容：`{job_id: {last_streak, last_error_hash, last_escalated_streak}}`
- 用途：评论节流、升级阈值判定（跨 tick 记忆）。损坏/缺失 → 视为空 dict 重建（不 crash）

### 5.5 模糊幂等（防与手工卡重复）
- 庄子手工建的修复卡（如 t_2eb6f13c / t_364fb671）**无 idempotency_key**，键探测命中不了。
- 解法（同 08-27 路由 agent 的建卡前检查）：建卡前对 `assignee=anran 且 status∉(done,archived) 且 idempotency_key IS NULL OR '' 且 title LIKE '%<job名>%'` 做只读探测；命中 → 视为该卡幂等命中（评论更新 streak，不建新卡）。
- **只限无 key 卡（C2 返工点，t_0ffcd12e 修复）**：模糊探测限定 `idempotency_key IS NULL OR idempotency_key=''`——自愈机制自身建的卡（带 `fail-<job_id>-<hash>` 键）**不参与**模糊命中。这样「错误变化 → 新 hash → 新键」时旧自愈卡（非 done）不会被模糊吞掉，按 §5.1 开新卡；§5.5 只防与手工卡重复，不干预错误变化路径。
- 与 5.1-5.4 的关系：键幂等（确定性）优先，命中键卡 → 按 5.3 评论文案；键未命中但模糊命中手工卡 → 同样只评论。两者皆无 → 建新卡。
- 风险：job 名过泛（如 "monitor"）可能误命中无关卡 → 评论无害（追加证据），宁齐勿漏。

## 六、分级升级（P1→P0）

| 触发 | 动作 |
|---|---|
| failure_streak ≥ `OPC_SELFHEAL_ESCALATE_STREAK`（默认 3） | ① 卡 title 前缀改 【P0 运维】+ priority 200（直连 UPDATE + edited 事件）；② 总线 append P0（source=cron-selfheal, type=cron-fail-escalate, fingerprint=cron-selfheal:cron-fail-escalate:YYYY-MM-DD, title=【P0】<job> 连续失败≥N 次, evidence=job 详情）→ **立即 mark_routed <fingerprint> <card_id>**；③ 卡 comment【升级 P0】留痕 |
| 首次建卡时 streak 已 ≥3 | 直接建 P0 卡（title 前缀 P0 + priority 200），同样 append P0 + mark_routed |

- **为什么 append 后立即 mark_routed**：路由 agent（5min tick）看到 status=new 会按自己逻辑建卡/建 G 卡，与自愈卡重复。mark_routed 立即把条目转 routed+card_id，路由 agent 的 read_new 看不到 → 无双卡。P0 事件仍留在总线（routed 态），进 18:50 每日汇总 → 19:00 日报 → 庄子主循环可见。满足 DoD「bus append P0 事件 + 庄子主循环可见」。
- **Gavin 知情路径**：沿用 08-27 定稿的 G 卡机制（路由 agent 对「需 Gavin 知情」类 P0 建 G 卡，工作台气泡+govern 提醒），本机制不新开 telegram 直推通道（无团队频道凭据；红线「绝不推 Gavin telegram」保持）。
- **降级/恢复**：卡 done（修复完成）后，总线来源条目由既有闭环 mark_resolved；自愈卡自身无需回写总线（它不是总线来源）。streak 归零由 cron 引擎管理，巡检不再见失败 → 静默。

## 七、输入面 B：总线陈旧回退路由（激活 08-27 定稿闭环）

- 读取 `opc-alert-bus.py read_new --json` 的 P0/P1 条目
- **仅处理 age ≥ `OPC_SELFHEAL_BUS_STALE_MIN`（默认 60min）的条目**——路由 agent 存活时 5min tick 早已处理（≤10min 内 routed），60min 仍 new = 路由 agent 挂/漏 → 自愈兜底。时间阈值消除与存活路由 agent 的竞态（双开卡）。
- 建卡：assignee=anran，级别=条目 severity（P0→P0 卡，P1→P1 卡），priority 200，title=【P1|P0 运维】<title>，body=总线原文 JSON（fingerprint/ts/evidence）+ 来源说明「自愈巡检回退路由（总线陈旧 >60min）」
- 幂等键：`bus-<fingerprint>`（总线 fingerprint 已是天然去重键：source:type:日期）
- 建卡后：`mark_routed <fingerprint> <card_id>` → 条目离开 new 集合
- **不建 G 卡**：G 卡（需 Gavin 知情类）是路由 agent 的判断职责；自愈只做机械兜底（anran 运维卡），判断留给庄子/路由 agent

## 八、脚本层数据容错规范（配套 · 本次两个故障的直接根因）

> 归宿：opc-operations skill（anran profile）+ 本设计文档；本次两张 P1 修复卡（t_2eb6f13c / t_364fb671）即示范。

| 场景 | 规范 | 本次故障映射 |
|---|---|---|
| 数据源返回非数字（空/N/A/`0 0`） | 算术前校验：`[ "$X" -eq "$X" ] 2>/dev/null` 或正则 `^[0-9]+$`；非数字→置 0 或跳过该轮，禁整脚本崩 | mrd_host_audit.sh 行41 `$((USERS - P_USERS))` 崩 |
| 未绑定变量（set -u） | 引用前判空：`${VAR:-0}` 默认值兜底 | mrd 行45 `D_REG_ERR: 未绑定的变量` |
| API/外部 CLI 失败（gh 等） | 前置 `export PATH/HOME` + 代理 env；失败→优雅降级（记录 not fatal），禁整崩 | four-platform-monitor gh token/网络全败 |
| 数据链路单点异常 | 缓存旧值 + 防抖告警（同指纹窗口内不重复炸） | 监控失效时保留旧 star/fork 数据 |
| 监控脚本与 kanban 之间 | 失败/告警二选一入总线或靠自愈巡检兜底，禁「stdout 直发个人 telegram」断链 | 两 watchdog 无卡无总线 |

## 九、静默与健壮性契约（红线）

1. **无失败/全部幂等命中且 streak 未变 → stdout 空**（no_agent 语义，零 token；deliver=local）
2. **有动作（建卡/升级/评论）→ stdout 一行摘要**（deliver=local 存档，不打扰任何人）
3. 单源失败隔离：一个 jobs.json 损坏/缺字段 → 跳过该源，不中断其余源
4. 外部调用全超时：hermes CLI 60s / bus 15s（同 system-monitor _bus_append 模式），失败静默降级
5. 巡检自身失败（如脚本 bug）→ 下一 tick cron 引擎记 last_status=error → 巡检扫到自己 → 自动建卡（机制自愈自己）。自身失败不 cascade：任何内部异常 catch-all 后 exit 0（stdout 空）
6. 禁止无限循环/风暴：幂等键 + 评论节流 + 升级一次性（state 记 last_escalated_streak）三重防刷

## 十、交付物与验收映射（DoD ↔ 蔡婉 t_407a8a23）

| DoD | 交付物 | 蔡婉验收点 |
|---|---|---|
| 设计文档 | 本文档 | C7（核查结论） |
| 巡检脚本 | cron-selfheal-monitor.py（~/.hermes/scripts/ 生产 + anran/scripts/ 注册副本 + opc-os/scripts/ 产品副本，三处内容一致） | C1-C5 |
| 本机验证 | 隔离环境演练记录（见卡 completion metadata） | C1-C4（她另做线上演练） |
| 产品包 | scripts/ + CHANGELOG[Unreleased] + delivery-checklist + installation-guide + 容错规范入 opc-operations + git commit+push（只含自己文件） | C6 |
| 总线链路核查 | §一/§二 结论：路由 cron 存活，断链点在脚本层，新机制输入面 A 补桥 | C7 |

## 十一、风险与兜底

- **路由 agent 与自愈双建卡竞态**：总线面只处理 >60min 陈旧条目 + 建卡后立即 mark_routed → 竞态窗口趋零；残余（同一秒双跑）由路由 agent 自身幂等（建卡前查 anran 非 done 卡）兜底
- **jobs.json 结构漂移**：扫描容错（缺字段跳过）；仅依赖 `id/enabled/last_status` 三个稳定字段，错误详情字段缺失时降级用 job 名
- **hermes CLI 在 cron 会话不可用**：绝对路径 + 60s 超时 + 失败静默（本 tick 放弃，下 tick 重试；不 cascade）
- **升级阈值误触发**（瞬时多 tick 连续失败）：streak≥3 才升 P0，且升级一次性（state 防重复 append P0）
- **版本**：CHANGELOG 入 [Unreleased]，不 bump VERSION（版本号三源对齐由 release.sh 在发布周期统一处理）
