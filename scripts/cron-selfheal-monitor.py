#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cron-selfheal-monitor —— 脚本型 cron 失败自动建卡 + 分级升级巡检（no_agent 静默模式）
========================================================================================
设计：docs/cron-selfheal-design.md（2026-08-29 定稿，卡 t_782e8d56）
职责（每 30min tick，anran profile cron，deliver=local）：
  输入面 A：扫描全部 cron jobs.json（default + ~/.hermes/profiles/*/）
            → enabled && last_status=error 的 job = 失败事件
  输入面 B：opc-alert-bus read_new 的 P0/P1 条目（仅 age>60min 陈旧条目，
            路由 agent 存活时早已处理；未路由 = 兜底建卡，激活 08-27 bus→建卡闭环）
  建卡：hermes kanban create CLI（官方操作面，assignee=anran，P1/P0，priority 200，
        idempotency_key=fail-<job_id>-<error_hash>；同 job 同错误不重复开卡）
  升级：failure_streak≥3 → 卡 title 前缀【P0 运维】+ priority 200（直连 UPDATE +
        task_events kind='edited' 留痕，唯一直接写例外）+ 总线 append P0 + mark_routed
  静默契约：无失败/无动作 → stdout 空（no_agent 语义，零 token）；有动作 → 一行摘要
  健壮性：单源失败隔离、外部调用全超时、自身异常 catch-all 后 exit 0（不 cascade）

env 参数化（Gavin 0827「公司即产品」，参照 system-monitor.py OPC_* 模式）：
  OPC_HOME                  默认 passwd 真实 home（cron 派生会话 HOME 被重定向，禁 expanduser）
  OPC_CRON_DIRS             默认 "{HOME}/.hermes/cron:{HOME}/.hermes/profiles/*/cron"
                            冒号分隔；含 * 时按 glob 展开（客户部署可显式指定）
  OPC_KANBAN_DB             默认 {HOME}/.hermes/kanban/boards/opc/kanban.db
  OPC_KANBAN_BOARD          默认 opc
  OPC_ALERT_DIR             默认 {HOME}/.hermes/opc/ops/alerts（同 opc-alert-bus.py）
  OPC_BUS_SCRIPT            默认 {HOME}/.hermes/scripts/opc-alert-bus.py
  OPC_HERMES_CLI            默认 {HOME}/.hermes/hermes-agent/venv/bin/hermes
  OPC_SELFHEAL_STATE        默认 {OPC_ALERT_DIR}/selfheal_state.json（评论节流/升级记忆）
  OPC_SELFHEAL_ESCALATE_STREAK  默认 3（streak≥N 升 P0）
  OPC_SELFHEAL_BUS_STALE_MIN    默认 60（总线条目 age≥N 分钟才回退路由）
  OPC_SELFHEAL_MAX_ERROR_LEN    默认 1500（body 内 last_error 截断）
  OPC_SELFHEAL_FRESH_HOURS      默认 6（失败新鲜度窗口：last_run_at 距今超过该小时数
                                的 error 视为历史残留跳过——低频 job 一次故障、配置已修
                                但状态未覆盖时，防止卡 done/归档后反复开新卡刷屏；
                                连续失败链 last_run_at 持续刷新永远在窗口内，不受影响）
  OPC_SELFHEAL_DRY_RUN      置 1 = 只打印将执行动作不写库（自测用）
  OPC_SELFHEAL_DEBUG        置 1 = 详细日志到 stderr

测试钩子：OPC_NOW='YYYY-MM-DD HH:MM:SS' 覆盖当前时间（验收/自测用，同 alerts_bus_monitor）。
"""
import glob
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta

# ---------- 常量（env 参数化，默认值=passwd 真实 home 派生，零硬编码） ----------
def _real_home():
    """真实用户 home（cron/agent 会话 HOME 被重定向，expanduser 不可靠）。"""
    try:
        import pwd
        return pwd.getpwuid(os.getuid()).pw_dir
    except Exception:
        return os.path.expanduser("~")


HOME = os.environ.get("OPC_HOME", _real_home())
_DEF_CRON_DIRS = f"{HOME}/.hermes/cron:{HOME}/.hermes/profiles/*/cron"
CRON_DIRS_ENV = os.environ.get("OPC_CRON_DIRS", _DEF_CRON_DIRS)
KANBAN_DB = os.environ.get("OPC_KANBAN_DB", f"{HOME}/.hermes/kanban/boards/opc/kanban.db")
KANBAN_BOARD = os.environ.get("OPC_KANBAN_BOARD", "opc")
ALERT_DIR = os.environ.get("OPC_ALERT_DIR", f"{HOME}/.hermes/opc/ops/alerts")
BUS_SCRIPT = os.environ.get("OPC_BUS_SCRIPT", f"{HOME}/.hermes/scripts/opc-alert-bus.py")
HERMES_CLI = os.environ.get("OPC_HERMES_CLI", f"{HOME}/.hermes/hermes-agent/venv/bin/hermes")
STATE_FILE = os.environ.get("OPC_SELFHEAL_STATE", os.path.join(ALERT_DIR, "selfheal_state.json"))
ESCALATE_STREAK = int(os.environ.get("OPC_SELFHEAL_ESCALATE_STREAK", "3"))
BUS_STALE_MIN = int(os.environ.get("OPC_SELFHEAL_BUS_STALE_MIN", "60"))
MAX_ERROR_LEN = int(os.environ.get("OPC_SELFHEAL_MAX_ERROR_LEN", "1500"))
FRESH_HOURS = int(os.environ.get("OPC_SELFHEAL_FRESH_HOURS", "6"))
DRY_RUN = os.environ.get("OPC_SELFHEAL_DRY_RUN", "") == "1"
DEBUG = os.environ.get("OPC_SELFHEAL_DEBUG", "") == "1"
ASSIGNEE = os.environ.get("OPC_SELFHEAL_ASSIGNEE", "anran")
CREATED_BY = os.environ.get("OPC_SELFHEAL_CREATED_BY", "cron-selfheal")
PRIORITY = int(os.environ.get("OPC_SELFHEAL_PRIORITY", "200"))
CLI_TIMEOUT = 60
BUS_TIMEOUT = 15

_TS_FMT = "%Y-%m-%d %H:%M:%S"


def _log(msg):
    if DEBUG:
        print(f"[selfheal] {msg}", file=sys.stderr)


def _now() -> datetime:
    ov = os.environ.get("OPC_NOW")
    if ov:
        try:
            return datetime.strptime(ov, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return datetime.now()


def _parse_iso(s):
    """ISO 8601 时间解析（jobs.json last_run_at 格式），失败返回 None。"""
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s))
    except ValueError:
        return None


def _parse_bus_ts(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, _TS_FMT)
    except ValueError:
        return None


def _error_hash(err: str) -> str:
    err = (err or "").strip()
    if not err:
        return "empty"
    return hashlib.sha256(err.encode("utf-8")).hexdigest()[:12]


def _err_summary(err: str, limit: int = 60) -> str:
    """last_error → 标题用一句话摘要：跳过脚本退出前缀，取首个有效行。"""
    if not err:
        return "exit=1"
    lines = [ln.strip() for ln in err.splitlines() if ln.strip()]
    for ln in lines:
        if re.match(r"^Script exited with code", ln) or ln == "stderr:":
            continue
        return ln[:limit]
    return lines[0][:limit] if lines else "exit=1"


# ---------- 输入面 A：扫描 cron jobs.json ----------
def _expand_cron_dirs() -> list:
    dirs = []
    for part in CRON_DIRS_ENV.split(":"):
        part = part.strip()
        if not part:
            continue
        if "*" in part:
            dirs.extend(sorted(glob.glob(part)))
        else:
            dirs.append(part)
    # 去重保序
    seen, out = set(), []
    for d in dirs:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def _profile_of_dir(cron_dir: str) -> str:
    """cron 目录 → profile 名（profiles/<name>/cron → name；否则 default）。"""
    m = re.search(r"profiles[/\\]([^/\\]+)[/\\]cron$", cron_dir)
    return m.group(1) if m else "default"


def _scan_jobs() -> list:
    """返回失败事件列表 [{dir, profile, job_id, name, script, last_error, last_run_at, streak}]。
    单文件损坏/缺字段 → 跳过该源，不中断其余源（红线：单源失败隔离）。"""
    events = []
    for cron_dir in _expand_cron_dirs():
        jobs_file = os.path.join(cron_dir, "jobs.json")
        if not os.path.isfile(jobs_file):
            continue
        try:
            with open(jobs_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            jobs = data.get("jobs", []) if isinstance(data, dict) else []
            if not isinstance(jobs, list):
                raise ValueError("jobs 非 list")
        except (OSError, ValueError, json.JSONDecodeError) as e:
            _log(f"跳过损坏 jobs.json {jobs_file}: {e}")
            continue
        profile = _profile_of_dir(cron_dir)
        for j in jobs:
            if not isinstance(j, dict):
                continue
            try:
                enabled = j.get("enabled", False)
                if enabled in (True, "true", "True", 1, "1"):
                    enabled = True
                else:
                    enabled = False
                if not enabled:
                    continue
                if j.get("last_status") != "error":
                    continue
                # 失败新鲜度过滤：last_run_at 距今 > FRESH_HOURS 的 error 视为历史残留
                # （故障早已恢复/低频 job 等待下次 run 覆盖状态），不重复报警——
                # 防 08-28 DNS 类一次失败在卡 done/归档后反复开新卡刷屏；
                # 连续失败链 last_run_at 持续刷新永远在窗口内，不受影响。
                lrt = _parse_iso(j.get("last_run_at"))
                if lrt is not None:
                    lrt_naive = lrt.replace(tzinfo=None) if lrt.tzinfo else lrt
                    age_h = (_now().replace(tzinfo=None) - lrt_naive).total_seconds() / 3600.0
                    if age_h > FRESH_HOURS:
                        _log(f"历史残留跳过 {j.get('id')} last_run_at={j.get('last_run_at')} age={age_h:.1f}h")
                        continue
                events.append({
                    "dir": cron_dir,
                    "profile": profile,
                    "job_id": str(j.get("id") or ""),
                    "name": str(j.get("name") or j.get("id") or "unknown"),
                    "script": str(j.get("script") or "") or None,
                    "last_error": str(j.get("last_error") or ""),
                    "last_run_at": str(j.get("last_run_at") or ""),
                    "streak": int(j.get("failure_streak") or 0),
                })
            except (TypeError, ValueError) as e:
                _log(f"跳过异常 job 记录 {jobs_file}: {e}")
                continue
    return events


# ---------- kanban：只读幂等探测 + 建卡/评论/升级 ----------
def _db_probe(key: str):
    """按 idempotency_key 探测已有卡（只读）。返回 dict(id,status,title,priority,completed_at) 或 None。
    db 不存在/不可读 → None（建卡交给 CLI 自身幂等，不阻塞）。"""
    if not os.path.isfile(KANBAN_DB):
        return None
    try:
        conn = sqlite3.connect(f"file:{KANBAN_DB}?mode=ro", uri=True, timeout=10)
        try:
            row = conn.execute(
                "SELECT id, status, title, priority, completed_at FROM tasks "
                "WHERE idempotency_key = ? AND status != 'archived' "
                "ORDER BY created_at DESC LIMIT 1",
                (key,),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        return {
            "id": row[0], "status": row[1], "title": row[2],
            "priority": row[3], "completed_at": row[4],
        }
    except sqlite3.Error as e:
        _log(f"db 只读探测失败 {key}: {e}")
        return None


def _resolve_key(base_key: str):
    """幂等键解析：命中 done 卡 → 追加确定性后缀 -r<completed_at>（回归复发开新卡）。
    返回 (final_key, existing_row_or_None)。单调收敛，10 轮保护。"""
    key = base_key
    for _ in range(10):
        row = _db_probe(key)
        if row is None or row["status"] != "done":
            return key, row
        key = f"{base_key}-r{row['completed_at']}"
    return key, None


def _find_existing_card(job_name: str):
    """模糊幂等：job 名已有一张 assignee 非 done 且【无 idempotency_key】的卡
    （庄子手工建的修复卡，无 key）→ 返回该卡（追加证据而非重复建卡）。
    限定无 key（C2 验收返工点，卡 t_0ffcd12e）：自愈机制自身建的卡带
    fail-<job_id>-<hash> 键，不参与模糊命中 → 错误变化（新 hash）按设计 §5.1
    开新卡，不被旧自愈卡吞掉；本函数只防与手工卡重复（设计 §5.5 本意）。
    同 08-27 路由 agent 的建卡前检查（assignee=anran 非 done + 标题含 job 名）。
    只读；LIKE 特殊字符转义。返回 dict(id,status,title,priority) 或 None。"""
    if not os.path.isfile(KANBAN_DB):
        return None
    escaped = (job_name or "").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    if not escaped:
        return None
    try:
        conn = sqlite3.connect(f"file:{KANBAN_DB}?mode=ro", uri=True, timeout=10)
        try:
            row = conn.execute(
                "SELECT id, status, title, priority FROM tasks "
                "WHERE assignee = ? AND status NOT IN ('done', 'archived') "
                "AND (idempotency_key IS NULL OR idempotency_key = '') "
                "AND title LIKE ? ESCAPE '\\' "
                "ORDER BY created_at DESC LIMIT 1",
                (ASSIGNEE, f"%{escaped}%"),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        return {"id": row[0], "status": row[1], "title": row[2], "priority": row[3]}
    except sqlite3.Error as e:
        _log(f"模糊幂等探测失败 {job_name}: {e}")
        return None


def _cli_env() -> dict:
    env = dict(os.environ)
    env["HERMES_KANBAN_DB"] = KANBAN_DB
    env["HERMES_KANBAN_BOARD"] = KANBAN_BOARD
    env["HOME"] = HOME
    return env


def _run_cli(args: list, timeout: int = CLI_TIMEOUT):
    """跑 hermes CLI，返回 (ok, stdout_text)。失败/超时 → (False, "")，调用方静默降级。"""
    try:
        p = subprocess.run(
            [HERMES_CLI] + args,
            capture_output=True, text=True, timeout=timeout,
            env=_cli_env(),
        )
        if p.returncode == 0:
            return True, p.stdout.strip()
        _log(f"CLI 失败 {args[:3]} rc={p.returncode}: {p.stderr[:300]}")
        return False, ""
    except (OSError, subprocess.TimeoutExpired) as e:
        _log(f"CLI 异常 {args[:3]}: {e}")
        return False, ""


def _create_card(title: str, body: str, key: str):
    """建卡（官方 CLI）。返回 card_id 或 None。"""
    ok, out = _run_cli([
        "kanban", "create", title,
        "--body", body,
        "--assignee", ASSIGNEE,
        "--priority", str(PRIORITY),
        "--created-by", CREATED_BY,
        "--idempotency-key", key,
        "--json",
    ])
    if not ok:
        return None
    try:
        return json.loads(out).get("id")
    except (ValueError, AttributeError):
        m = re.search(r"\"id\":\s*\"(t_[0-9a-f]+)\"", out)
        return m.group(1) if m else None


def _add_comment(card_id: str, text: str):
    _run_cli(["kanban", "comment", card_id, text, "--author", CREATED_BY], timeout=30)


def _db_update_meta(card_id: str, title: str, priority: int, old_title: str, old_priority):
    """P0 升级唯一直接写例外：UPDATE title/priority + task_events(kind='edited') 留痕。
    官方 CLI 无改 title/priority 能力（hermes kanban edit 只支持 result/summary/metadata）。
    元数据变更，非状态机迁移（status/claim/run 不动）。失败静默（升级以评论为准）。"""
    if DRY_RUN:
        return True
    try:
        conn = sqlite3.connect(KANBAN_DB, timeout=10)
        try:
            conn.execute(
                "UPDATE tasks SET title = ?, priority = ? WHERE id = ?",
                (title, priority, card_id),
            )
            payload = json.dumps({
                "field": "escalate_p0",
                "title": {"from": old_title, "to": title},
                "priority": {"from": old_priority, "to": priority},
                "by": CREATED_BY,
            }, ensure_ascii=False)
            conn.execute(
                "INSERT INTO task_events (task_id, run_id, kind, payload, created_at) "
                "VALUES (?, NULL, 'edited', ?, ?)",
                (card_id, payload, int(time.time())),
            )
            conn.commit()
        finally:
            conn.close()
        return True
    except sqlite3.Error as e:
        _log(f"升级写库失败 {card_id}: {e}")
        return False


# ---------- 告警总线（输入面 B + P0 升级事件） ----------
def _bus(args: list):
    try:
        subprocess.run(
            [sys.executable, BUS_SCRIPT] + args,
            capture_output=True, text=True, timeout=BUS_TIMEOUT,
            env=dict(os.environ, OPC_ALERT_DIR=ALERT_DIR),
        )
    except (OSError, subprocess.TimeoutExpired):
        pass  # 总线故障不影响主流程（_bus_append 模式）


def _bus_read_new() -> list:
    if not os.path.isfile(BUS_SCRIPT):
        return []
    try:
        p = subprocess.run(
            [sys.executable, BUS_SCRIPT, "read_new", "--json"],
            capture_output=True, text=True, timeout=BUS_TIMEOUT,
            env=dict(os.environ, OPC_ALERT_DIR=ALERT_DIR),
        )
    except (OSError, subprocess.TimeoutExpired):
        return []
    rows = []
    for line in p.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except ValueError:
            continue
    return rows


def _bus_append(severity, type_, title, evidence, fingerprint):
    _bus(["append", "--source", "cron-selfheal", "--severity", severity,
          "--type", type_, "--title", title, "--evidence", str(evidence)[:1500],
          "--fingerprint", fingerprint])


def _bus_mark_routed(fingerprint, card_id):
    _bus(["mark_routed", fingerprint, card_id])


# ---------- state（评论节流 / 升级一次性记忆） ----------
def _load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except (OSError, ValueError, json.JSONDecodeError):
        return {}


def _save_state(d: dict):
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=1)
    except OSError:
        pass  # state 写失败不阻塞主流程


# ---------- 动作执行 ----------
def _card_body(ev: dict) -> str:
    err = ev["last_error"][:MAX_ERROR_LEN]
    lines = [
        "【自愈巡检自动建卡 · cron-selfheal-monitor】",
        f"- job_id: {ev['job_id']}",
        f"- job 名: {ev['name']}",
        f"- profile: {ev['profile']}",
        f"- script: {ev['script'] or 'N/A'}",
        f"- last_run_at: {ev['last_run_at'] or 'N/A'}",
        f"- failure_streak: {ev['streak']}",
        "- last_error:",
        err if err else "(空)",
        "",
        "【修复提示 · 脚本层数据容错规范】",
        "1. 数据源非数字（空/N/A/多值）→ 算术前校验，非数字置 0 或跳过该轮，禁整脚本崩",
        "2. set -u 下未绑定变量 → 引用前 ${VAR:-0} 兜底",
        "3. 外部 CLI（gh 等）→ 绝对路径 + PATH/HOME/代理 env；失败优雅降级（记录 not fatal）",
        "4. API 异常 → 缓存旧值 + 防抖告警，不因单点数据异常整条监控失效",
        "5. 修好即 done（自动派活 assignee=" + ASSIGNEE + "）；需排查协助走卡评论",
    ]
    return "\n".join(lines)


def _process_job_failure(ev: dict, state: dict, actions: list):
    job_id = ev["job_id"]
    if not job_id:
        return
    err_hash = _error_hash(ev["last_error"])
    base_key = f"fail-{job_id}-{err_hash}"
    key, row = _resolve_key(base_key)
    streak = ev["streak"]
    p0 = streak >= ESCALATE_STREAK
    lvl = "P0" if p0 else "P1"
    title = f"【{lvl} 运维】{ev['name']} 失败：{_err_summary(ev['last_error'])}"
    st = state.get(job_id, {})

    if row is None:
        # 新卡前先查模糊幂等：job 名已有非 done 卡（手工建的修复卡无 key）→ 追加证据不重复建卡
        fuzzy = _find_existing_card(ev["name"])
        if fuzzy is not None:
            _log(f"模糊幂等命中 {fuzzy['id']}（标题含 job 名，跳过建卡）")
            row = fuzzy

    if DRY_RUN:
        if row is None:
            actions.append(f"[dry-run] 将建卡 {title} (key={key}, {lvl})")
        elif row["status"] != "done":
            actions.append(f"[dry-run] 幂等命中 {row['id']}，streak {st.get('last_streak', 0)}→{streak}")
        return

    if row is None:
        # 新卡
        card_id = _create_card(title, _card_body(ev), key)
        if card_id:
            actions.append(f"建卡 {card_id}（{lvl}）：{ev['name']} streak={streak}")
            # 提案7 §四：cron 失败入链（不只 stderr）——普通失败也 append P1 总线，
            # fingerprint 按 job 幂等（同日同 job 一条），路由 agent/总线可见
            _bus_append(lvl, "cron-fail",
                        f"【{lvl}】{ev['name']} 失败：{_err_summary(ev['last_error'])}",
                        f"job_id={job_id} profile={ev['profile']} streak={streak} card={card_id}",
                        f"cron-selfheal:cron-fail:{job_id}:{_now().strftime('%Y-%m-%d')}")
            if p0:
                _bus_append("P0", "cron-fail-escalate",
                            f"【P0】{ev['name']} 连续失败 {streak} 次（自愈升级）",
                            f"job_id={job_id} profile={ev['profile']} streak={streak} card={card_id}",
                            f"cron-selfheal:cron-fail-escalate:{_now().strftime('%Y-%m-%d')}")
                _bus_mark_routed(f"cron-selfheal:cron-fail-escalate:{_now().strftime('%Y-%m-%d')}", card_id)
                actions.append(f"总线 P0 事件落盘并回写 card={card_id}")
            state[job_id] = {"last_streak": streak, "last_error_hash": err_hash,
                             "last_escalated_streak": streak if p0 else 0}
        else:
            actions.append(f"建卡失败（CLI 异常，下轮重试）：{ev['name']}")
        return

    # 幂等命中（卡存在且非 done）
    card_id = row["id"]
    updated = False
    if streak > int(st.get("last_streak", 0)):
        _add_comment(card_id, f"【自愈巡检】第 {streak} 次失败（{ev['last_run_at'] or '?'}）streak={streak}")
        actions.append(f"评论更新 {card_id}（streak {st.get('last_streak', 0)}→{streak}）")
        updated = True
    if p0 and int(st.get("last_escalated_streak", 0)) < streak:
        old_title = row["title"]
        new_title = title
        _db_update_meta(card_id, new_title, PRIORITY, old_title, row["priority"])
        _add_comment(card_id, f"【升级 P0】连续失败 {streak} 次 ≥{ESCALATE_STREAK}，title/priority 已升级")
        _bus_append("P0", "cron-fail-escalate",
                    f"【P0】{ev['name']} 连续失败 {streak} 次（自愈升级）",
                    f"job_id={job_id} profile={ev['profile']} streak={streak} card={card_id}",
                    f"cron-selfheal:cron-fail-escalate:{_now().strftime('%Y-%m-%d')}")
        _bus_mark_routed(f"cron-selfheal:cron-fail-escalate:{_now().strftime('%Y-%m-%d')}", card_id)
        actions.append(f"升级 {card_id} → P0（streak={streak}）+ 总线 P0 回写")
        updated = True
    state[job_id] = {"last_streak": max(int(st.get("last_streak", 0)), streak),
                     "last_error_hash": err_hash,
                     "last_escalated_streak": max(int(st.get("last_escalated_streak", 0)),
                                                  streak if p0 else 0)}
    if not updated:
        _log(f"幂等命中 {card_id} 无变化（streak={streak}）")


def _process_bus_entries(now: datetime, actions: list):
    """输入面 B：陈旧（age≥BUS_STALE_MIN）未路由的 P0/P1 条目 → 兜底建卡 + mark_routed。"""
    for e in _bus_read_new():
        sev = str(e.get("severity", "P1")).upper()
        if sev not in ("P0", "P1"):
            continue
        ts = _parse_bus_ts(e.get("ts"))
        if ts is None:
            continue
        age_min = (now - ts).total_seconds() / 60.0
        if age_min < BUS_STALE_MIN:
            continue  # 路由 agent 正常窗口内，不抢活
        fp = str(e.get("fingerprint") or "")
        if not fp:
            continue
        title = str(e.get("title") or "未命名告警")
        key, row = _resolve_key(f"bus-{fp}")
        card_id = None
        if row is None:
            body_lines = [
                "【自愈巡检 · 总线陈旧回退路由】",
                f"- 来源: {e.get('source', '?')}",
                f"- severity: {sev}",
                f"- fingerprint: {fp}",
                f"- ts: {e.get('ts', '?')}",
                f"- age: {int(age_min)}min ≥ {BUS_STALE_MIN}min（路由 agent 未在窗口内处理 → 机械兜底）",
                "- 总线原文:",
                json.dumps(e, ensure_ascii=False)[:1200],
                "",
                "【修复提示】总线告警对应组件排查修复，修好即 done；恢复回写总线 mark_resolved。",
            ]
            if DRY_RUN:
                actions.append(f"[dry-run] 总线回退建卡 {title} (key={key})")
                continue
            card_id = _create_card(f"【{sev} 运维】{title}", "\n".join(body_lines), key)
            if card_id:
                actions.append(f"总线回退建卡 {card_id}（{sev}）：{title}（陈旧 {int(age_min)}min）")
        else:
            card_id = row["id"]
            if row["status"] == "done":
                continue
        if card_id:
            _bus_mark_routed(fp, card_id)


# ---------- main ----------
def main():
    now = _now()
    actions = []
    state = {} if DRY_RUN else _load_state()

    # 输入面 A：cron 失败采集
    events = _scan_jobs()
    _log(f"扫描失败事件 {len(events)} 条")
    for ev in events:
        try:
            _process_job_failure(ev, state, actions)
        except Exception as e:  # 单 job 异常不 cascade（红线）
            _log(f"处理 job 失败异常 {ev.get('job_id')}: {e}")

    # 输入面 B：总线陈旧回退
    try:
        _process_bus_entries(now, actions)
    except Exception as e:
        _log(f"总线面异常: {e}")

    if not DRY_RUN and state:
        _save_state(state)

    # 静默契约：无动作 → stdout 空（no_agent 零 token）；有动作 → 一行摘要
    if actions:
        print(" | ".join(actions))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # 自身异常 catch-all：exit 0 + 静默（巡检失败不 cascade）
        if DEBUG:
            print(f"[selfheal] 致命异常: {e}", file=sys.stderr)
        sys.exit(0)
