"""时限档变更后重裁 —— 库文件与写路径。

口径（不可偏离）：
- 改档只能走「改时限档 → 重裁到达」这一条路；不做纯显示切换。
- 重裁路径只有甲、乙两种，同一裁决卡上不得混用：
  · 甲：改档与旧到达重裁同笔事务写入，遇错整体回滚；
  · 乙：旧到达先归档为历史快照，再按新档强制复检，全部通过后才改输出态；
        中断时快照保留、输出态保持未改。
- 裁决卡一经写入即冻结，当天不得更换；偏离裁决卡即驳回并留「重裁偏了」痕。
- 到达主键全程保留；改档后源目录、到达列表、策略附页、水位页、历史目录五处同按新档。
- 配置员与补采员改同一源须互斥占槽，后手只留重裁争用痕。
"""

from datetime import datetime

from fastapi import HTTPException

PATHS = ("甲", "乙")

READJUDICATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS sla_policies(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  old_sla INTEGER NOT NULL,
  new_sla INTEGER NOT NULL,
  path TEXT NOT NULL,
  tier_version INTEGER NOT NULL,
  card_id INTEGER,
  changed_by TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS arrival_history(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  arrival_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  source_clock TEXT NOT NULL,
  ingest_clock TEXT NOT NULL,
  lag_minutes INTEGER NOT NULL,
  old_level TEXT NOT NULL,
  new_level TEXT NOT NULL,
  old_sla INTEGER NOT NULL,
  adjudicated_sla INTEGER NOT NULL,
  stage TEXT NOT NULL,
  card_id INTEGER NOT NULL,
  snapshot_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS readjudication_cards(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  path TEXT NOT NULL,
  old_sla INTEGER NOT NULL,
  new_sla INTEGER NOT NULL,
  state TEXT NOT NULL,
  frozen INTEGER NOT NULL DEFAULT 1,
  day TEXT NOT NULL,
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  executed_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_card_day ON readjudication_cards(source, day);
CREATE TABLE IF NOT EXISTS readjudication_audit(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  card_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  path TEXT NOT NULL,
  action TEXT NOT NULL,
  detail TEXT NOT NULL,
  at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS readjudication_deviations(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  card_id INTEGER NOT NULL,
  source TEXT NOT NULL,
  attempted_path TEXT NOT NULL,
  card_path TEXT NOT NULL,
  detail TEXT NOT NULL,
  at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS readjudication_contention(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  slot_owner TEXT NOT NULL,
  contender TEXT NOT NULL,
  contender_role TEXT NOT NULL,
  action TEXT NOT NULL,
  detail TEXT NOT NULL,
  at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS source_slots(
  source TEXT PRIMARY KEY,
  holder TEXT NOT NULL,
  holder_role TEXT NOT NULL,
  action TEXT NOT NULL,
  acquired_at REAL NOT NULL,
  ttl REAL NOT NULL
);
"""


def add_column(conn, table: str, column: str, decl: str):
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_schema(conn, now=None):
    """建重裁表、补列、给老库补裁决档与水位（不改既有到达主键）。"""
    now = now or _now()
    add_column(conn, "sources", "tier_version", "INTEGER NOT NULL DEFAULT 1")
    add_column(conn, "sources", "watermark_arrival_id", "INTEGER")
    add_column(conn, "sources", "watermark_at", "TEXT")
    add_column(conn, "arrivals", "adjudicated_sla", "INTEGER")
    add_column(conn, "arrivals", "adjudicated_at", "TEXT")
    conn.executescript(READJUDICATION_SCHEMA)
    conn.execute("UPDATE arrivals SET adjudicated_sla=(SELECT sla_minutes FROM sources WHERE sources.name=arrivals.source) WHERE adjudicated_sla IS NULL")
    conn.execute("""UPDATE sources SET
          watermark_arrival_id=(SELECT MAX(id) FROM arrivals WHERE arrivals.source=sources.name),
          watermark_at=COALESCE(watermark_at, ?)
        WHERE watermark_arrival_id IS NULL
          AND EXISTS (SELECT 1 FROM arrivals WHERE arrivals.source=sources.name)""", (now,))


def level_of(lag, sla):
    if lag <= sla: return "正常"
    if lag <= sla * 2: return "告警"
    return "违约"


def mode_of(level):
    return {"正常": "可写", "告警": "降级只读", "违约": "冻结"}[level]


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


# ---------------- 痕 / 卡 / 槽 ----------------

def audit(conn, card_id, source, path, action, detail, at=None):
    conn.execute("INSERT INTO readjudication_audit(card_id,source,path,action,detail,at) VALUES (?,?,?,?,?,?)",
                 (card_id, source, path, action, detail, at or _now()))


def latest_card(conn, source):
    return conn.execute("SELECT * FROM readjudication_cards WHERE source=? ORDER BY id DESC LIMIT 1",
                        (source,)).fetchone()


def acquire_slot(conn, source, holder, role, action, hold_seconds=0.0):
    """配置员与补采员对同一源互斥占槽；后手只留重裁争用痕，不改任何业务状态。

    槽以 source 为主键落在库里，跨进程也互斥：两个进程同时抢时，后到者的
    INSERT 会撞主键，此时同样回退为 409 并留争用痕，而不是冒 500。
    """
    import sqlite3, time
    while True:
        now = time.time()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute("SELECT * FROM source_slots WHERE source=?", (source,)).fetchone()
            if row and now - row["acquired_at"] < row["ttl"]:
                conn.execute("ROLLBACK")
                _contention(conn, source, row["holder"], row["holder_role"], holder, role, action)
                raise HTTPException(409, "重裁占槽冲突")
            conn.execute("DELETE FROM source_slots WHERE source=?", (source,))
            conn.execute("INSERT INTO source_slots(source,holder,holder_role,action,acquired_at,ttl) VALUES (?,?,?,?,?,?)",
                         (source, holder, role, action, now, max(hold_seconds, 0.0) + 60))
            conn.execute("COMMIT")
            if hold_seconds:
                time.sleep(hold_seconds)
            return
        except HTTPException:
            raise
        except sqlite3.IntegrityError:
            # 另一进程刚把槽插进去了，转身留争用痕
            conn.execute("ROLLBACK")
            owner = conn.execute("SELECT * FROM source_slots WHERE source=?", (source,)).fetchone()
            _contention(conn, source, owner["holder"] if owner else "?",
                        owner["holder_role"] if owner else "?", holder, role, action)
            raise HTTPException(409, "重裁占槽冲突")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def _contention(conn, source, owner, owner_role, contender, contender_role, action):
    conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        "INSERT INTO readjudication_contention(source,slot_owner,contender,contender_role,action,detail,at) VALUES (?,?,?,?,?,?,?)",
        (source, owner, contender, contender_role, action,
         f"{contender_role}与{owner_role}争用同一源，后手未落地", _now()))
    conn.execute("COMMIT")


def release_slot(conn, source, holder):
    conn.execute("DELETE FROM source_slots WHERE source=? AND holder=?", (source, holder))


def capture_watermark(conn, source):
    row = conn.execute("SELECT MAX(id) mid FROM arrivals WHERE source=?", (source,)).fetchone()
    return row["mid"], _now()


def failure_trace(connect, card_id, source, path, detail):
    """回滚后另起一笔只写痕，不让失败本身被回滚掉。"""
    conn = connect()
    try:
        audit(conn, card_id, source, path, "回滚", detail)
        conn.execute("UPDATE readjudication_cards SET state='执行失败', frozen=1 WHERE id=?", (card_id,))
    finally:
        conn.close()


def traces(conn):
    return {
        "audit": [dict(r) for r in conn.execute("SELECT * FROM readjudication_audit ORDER BY id DESC").fetchall()],
        "deviations": [dict(r) for r in conn.execute("SELECT * FROM readjudication_deviations ORDER BY id DESC").fetchall()],
        "contention": [dict(r) for r in conn.execute("SELECT * FROM readjudication_contention ORDER BY id DESC").fetchall()],
    }


# ---------------- 甲 / 乙 写路径 ----------------

def _fault(fault, stage):
    if fault is not None:
        fault(stage)


def run_path_a(conn, src, card, new_sla, user, fault=None, connect=None):
    """甲：改档 + 旧到达重裁同笔事务；任何一步出错整体回滚。"""
    if new_sla == src["sla_minutes"]:
        raise HTTPException(422, "新档与原档相同，无需重裁")
    conn.execute("BEGIN IMMEDIATE")
    try:
        version = src["tier_version"] + 1
        conn.execute("UPDATE sources SET sla_minutes=?, tier_version=? WHERE name=?",
                     (new_sla, version, src["name"]))
        rows = conn.execute("SELECT * FROM arrivals WHERE source=? ORDER BY id", (src["name"],)).fetchall()
        for row in rows:
            _fault(fault, "recompute")
            lv = level_of(row["lag_minutes"], new_sla)
            conn.execute("UPDATE arrivals SET level=?, adjudicated_sla=?, adjudicated_at=? WHERE id=?",
                         (lv, new_sla, _now(), row["id"]))
            conn.execute(
                "INSERT INTO arrival_history(arrival_id,source,source_clock,ingest_clock,lag_minutes,old_level,new_level,old_sla,adjudicated_sla,stage,card_id,snapshot_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (row["id"], row["source"], row["source_clock"], row["ingest_clock"], row["lag_minutes"],
                 row["level"], lv, row["adjudicated_sla"], new_sla, "已复检", card["id"], _now()))
        wm_id, wm_at = capture_watermark(conn, src["name"])
        latest_mode = mode_of(level_of(rows[-1]["lag_minutes"], new_sla)) if rows else src["output_mode"]
        conn.execute("UPDATE sources SET output_mode=?, watermark_arrival_id=?, watermark_at=? WHERE name=?",
                     (latest_mode, wm_id, wm_at, src["name"]))
        conn.execute("INSERT INTO sla_policies(source,old_sla,new_sla,path,tier_version,card_id,changed_by,created_at) VALUES (?,?,?,?,?,?,?,?)",
                     (src["name"], src["sla_minutes"], new_sla, "甲", version, card["id"], user, _now()))
        conn.execute("UPDATE readjudication_cards SET state='已执行', executed_at=? WHERE id=?", (_now(), card["id"]))
        audit(conn, card["id"], src["name"], "甲", "执行", f"改档 {src['sla_minutes']}→{new_sla}，重裁 {len(rows)} 条到达")
        _fault(fault, "commit")
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        if not isinstance(e, HTTPException):
            failure_trace(connect, card["id"], src["name"], "甲", f"甲路径半截失败整体回滚：{e}")
            raise HTTPException(500, "重裁失败，已整体回滚")
        raise


def run_path_b(conn, src, card, new_sla, user, fault=None, connect=None):
    """乙：先把旧到达归档为历史快照，再按新档强制复检；中断则快照保留、输出态保持未改。"""
    if new_sla == src["sla_minutes"]:
        raise HTTPException(422, "新档与原档相同，无需重裁")
    rows = conn.execute("SELECT * FROM arrivals WHERE source=? ORDER BY id", (src["name"],)).fetchall()
    # 1) 快照先落（独立提交，中断也保留）
    conn.execute("BEGIN IMMEDIATE")
    for row in rows:
        old_sla = row["adjudicated_sla"] if row["adjudicated_sla"] is not None else src["sla_minutes"]
        conn.execute(
            "INSERT INTO arrival_history(arrival_id,source,source_clock,ingest_clock,lag_minutes,old_level,new_level,old_sla,adjudicated_sla,stage,card_id,snapshot_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (row["id"], row["source"], row["source_clock"], row["ingest_clock"], row["lag_minutes"],
             row["level"], row["level"], old_sla, old_sla, "待复检", card["id"], _now()))
    conn.execute("COMMIT")
    # 2) 强制复检全部通过后才动档与输出态
    try:
        _fault(fault, "recheck")
        checked = [(row, level_of(row["lag_minutes"], new_sla)) for row in rows]
    except Exception as e:
        audit(conn, card["id"], src["name"], "乙", "中断", f"复检中断，快照保留、输出态未改：{e}")
        raise HTTPException(500, "重裁复检中断，快照已保留")
    conn.execute("BEGIN IMMEDIATE")
    try:
        version = src["tier_version"] + 1
        conn.execute("UPDATE sources SET sla_minutes=?, tier_version=? WHERE name=?",
                     (new_sla, version, src["name"]))
        for row, new_level in checked:
            conn.execute("UPDATE arrivals SET level=?, adjudicated_sla=?, adjudicated_at=? WHERE id=?",
                         (new_level, new_sla, _now(), row["id"]))
            conn.execute("UPDATE arrival_history SET new_level=?, adjudicated_sla=?, stage='已复检' WHERE card_id=? AND arrival_id=?",
                         (new_level, new_sla, card["id"], row["id"]))
        wm_id, wm_at = capture_watermark(conn, src["name"])
        latest_mode = mode_of(checked[-1][1]) if checked else src["output_mode"]
        conn.execute("UPDATE sources SET output_mode=?, watermark_arrival_id=?, watermark_at=? WHERE name=?",
                     (latest_mode, wm_id, wm_at, src["name"]))
        conn.execute("INSERT INTO sla_policies(source,old_sla,new_sla,path,tier_version,card_id,changed_by,created_at) VALUES (?,?,?,?,?,?,?,?)",
                     (src["name"], src["sla_minutes"], new_sla, "乙", version, card["id"], user, _now()))
        conn.execute("UPDATE readjudication_cards SET state='已执行', executed_at=? WHERE id=?", (_now(), card["id"]))
        audit(conn, card["id"], src["name"], "乙", "执行", f"快照归档后复检通过，改档 {src['sla_minutes']}→{new_sla}，重裁 {len(checked)} 条到达")
        _fault(fault, "commit")
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        if not isinstance(e, HTTPException):
            failure_trace(connect, card["id"], src["name"], "乙", f"复检通过后落地失败，快照保留：{e}")
            raise HTTPException(500, "重裁失败，快照保留")
        raise


def execute_card(conn, card, src, path, user, fault=None, connect=None):
    if path not in PATHS:
        raise HTTPException(422, "路径只能是甲或乙，甲乙不得混用")
    if path != card["path"]:
        conn.execute("INSERT INTO readjudication_deviations(card_id,source,attempted_path,card_path,detail,at) VALUES (?,?,?,?,?,?)",
                     (card["id"], card["source"], path, card["path"], "偏离时限重裁裁决卡：重裁偏了", _now()))
        audit(conn, card["id"], card["source"], path, "驳回", "偏离裁决卡，记下重裁偏了")
        raise HTTPException(422, "偏离时限重裁裁决卡，已驳回并记下重裁偏了")
    if card["state"] == "已执行":
        raise HTTPException(409, "该裁决卡已执行完毕")
    if path == "甲":
        run_path_a(conn, src, card, card["new_sla"], user, fault=fault, connect=connect)
    else:
        run_path_b(conn, src, card, card["new_sla"], user, fault=fault, connect=connect)


def open_card(conn, source, path, old_sla, new_sla, user, day=None):
    """写入即冻结：当天同一源只许一张卡。"""
    import sqlite3
    if path not in PATHS:
        raise HTTPException(422, "时限档变更必须走重裁，且只能选甲或乙一条路径")
    day = day or today()
    latest = latest_card(conn, source)
    if latest and latest["day"] == day:
        raise HTTPException(409, "当天已有重裁裁决卡，冻结后不可更换")
    try:
        conn.execute("INSERT INTO readjudication_cards(source,path,old_sla,new_sla,state,frozen,day,created_by,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
                     (source, path, old_sla, new_sla, "待执行", 1, day, user, _now()))
    except sqlite3.IntegrityError:
        # 唯一索引兜底：并发下另一笔已抢先写卡
        raise HTTPException(409, "当天已有重裁裁决卡，冻结后不可更换")
    return latest_card(conn, source)
