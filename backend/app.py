
import hashlib, secrets, sqlite3
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

from readjudication.core import (
    PATHS, init_schema, level_of, mode_of, acquire_slot, release_slot,
    traces as readjudication_traces_of, execute_card, open_card,
)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}
# 测试缝：注入半截失败 / 拉长占槽窗口，用于验证回滚与双端争用
_FAULT_HOOK = None
_SLOT_HOLD_SECONDS = 0.0

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users WHERE username='admin'").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
    for u, p, r in (("viewer", "view123", "只读"), ("ops", "123456", "补采员"), ("config", "123456", "配置员")):
        if conn.execute("SELECT COUNT(*) c FROM users WHERE username=?", (u,)).fetchone()["c"] == 0:
            conn.execute("INSERT INTO users VALUES (?,?,?)", (u, sha(p), r))

ROLE_ADMIN, ROLE_CONFIG, ROLE_CATCHUP = "管理员", "配置员", "补采员"

def role_of(conn, username: str) -> str:
    row = conn.execute("SELECT role FROM users WHERE username=?", (username,)).fetchone()
    return row["role"] if row else ""

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

def require_role(conn, username: str, *allowed: str) -> str:
    role = role_of(conn, username)
    if role not in allowed:
        raise HTTPException(403, "无权限")
    return role

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "源到达SLA"
SKELETON = "多源到达时钟对照 SLA 分级，超限则冻结或降级输出，补齐后恢复并对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS sources(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT UNIQUE NOT NULL,
          sla_minutes INTEGER NOT NULL,
          output_mode TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS arrivals(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          source_clock TEXT NOT NULL,
          ingest_clock TEXT NOT NULL,
          lag_minutes INTEGER NOT NULL,
          level TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS catchups(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          source TEXT NOT NULL,
          status TEXT NOT NULL,
          restored_mode TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        if conn.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"] == 0:
            conn.executemany("INSERT INTO sources(name,sla_minutes,output_mode) VALUES (?,?,?)", [
                ("订单源", 15, "可写"), ("支付源", 20, "降级只读"), ("物流源", 30, "冻结"),
            ])
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("支付源","2026-11-16T08:00:00","2026-11-16T08:35:00",35,"告警"))
            conn.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level) VALUES (?,?,?,?,?)",
                         ("物流源","2026-11-16T07:00:00","2026-11-16T09:10:00",130,"违约"))
        # 老库补齐：改档列、重裁表、既有到达裁决档与水位（不改既有主键）
        init_schema(conn, now=_now())

def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")

@app.on_event("startup")
def _s(): init_db()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "sources": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/sources")
def sources(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM sources ORDER BY id").fetchall()]}

class ArrivalIn(BaseModel):
    source: str
    source_clock: str
    ingest_clock: str

@app.post("/api/arrivals")
def add_arrival(body: ArrivalIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        src = c.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        # lag by minute rough parse
        def parse(t):
            return datetime.fromisoformat(t)
        lag = int((parse(body.ingest_clock) - parse(body.source_clock)).total_seconds() // 60)
        if lag < 0: lag = 0
        lv = level_of(lag, src["sla_minutes"])
        mode = mode_of(lv)
        c.execute("INSERT INTO arrivals(source,source_clock,ingest_clock,lag_minutes,level,adjudicated_sla,adjudicated_at) VALUES (?,?,?,?,?,?,?)",
                  (body.source, body.source_clock, body.ingest_clock, lag, lv, src["sla_minutes"], _now()))
        c.execute("UPDATE sources SET output_mode=?, watermark_arrival_id=(SELECT MAX(id) FROM arrivals WHERE source=?), watermark_at=? WHERE name=?",
                  (mode, body.source, _now(), body.source))
        return {"lag_minutes": lag, "level": lv, "output_mode": mode}

@app.get("/api/arrivals")
def arrivals(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM arrivals ORDER BY id DESC").fetchall()]}

@app.post("/api/catchup/{source}")
def catchup(source: str, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    with connect() as c:
        require_role(c, user, ROLE_ADMIN, ROLE_CATCHUP)
        acquire_slot(c, source, user, role_of(c, user), "catchup", hold_seconds=_SLOT_HOLD_SECONDS)
        try:
            src = c.execute("SELECT * FROM sources WHERE name=?", (source,)).fetchone()
            if not src: raise HTTPException(404, "未知源")
            c.execute("UPDATE sources SET output_mode='可写' WHERE name=?", (source,))
            c.execute("INSERT INTO catchups(source,status,restored_mode,note,created_at) VALUES (?,?,?,?,?)",
                      (source, "成功", "可写", "补齐后恢复可写", _now()))
        finally:
            release_slot(c, source, user)
        return {"ok": True, "output_mode": "可写"}

class ReadjudicationIn(BaseModel):
    source: str
    path: str | None = None
    sla_minutes: int

@app.post("/api/readjudications")
def create_readjudication(body: ReadjudicationIn, authorization: str | None = Header(default=None)):
    """配置入口：配置员/管理员为某源写入时限重裁裁决卡并当场执行。"""
    user = require_user(authorization)
    with connect() as c:
        require_role(c, user, ROLE_ADMIN, ROLE_CONFIG)
        if body.path not in PATHS:
            raise HTTPException(422, "时限档变更必须走重裁，且只能选甲或乙一条路径")
        src = c.execute("SELECT * FROM sources WHERE name=?", (body.source,)).fetchone()
        if not src: raise HTTPException(404, "未知源")
        if body.sla_minutes == src["sla_minutes"]:
            raise HTTPException(422, "新档与原档相同，无需重裁")
        acquire_slot(c, body.source, user, role_of(c, user), "readjudication", hold_seconds=_SLOT_HOLD_SECONDS)
        try:
            card = open_card(c, body.source, body.path, src["sla_minutes"], body.sla_minutes, user)
            execute_card(c, card, src, body.path, user, fault=_FAULT_HOOK, connect=connect)
            card = c.execute("SELECT * FROM readjudication_cards WHERE id=?", (card["id"],)).fetchone()
            return {"ok": True, "card_id": card["id"], "path": card["path"], "state": card["state"],
                    "sla_minutes": body.sla_minutes}
        finally:
            release_slot(c, body.source, user)

class ExecuteIn(BaseModel):
    path: str

@app.post("/api/readjudications/{card_id}/execute")
def execute_readjudication(card_id: int, body: ExecuteIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    with connect() as c:
        require_role(c, user, ROLE_ADMIN, ROLE_CONFIG)
        card = c.execute("SELECT * FROM readjudication_cards WHERE id=?", (card_id,)).fetchone()
        if not card: raise HTTPException(404, "裁决卡不存在")
        src = c.execute("SELECT * FROM sources WHERE name=?", (card["source"],)).fetchone()
        acquire_slot(c, card["source"], user, role_of(c, user), "readjudication", hold_seconds=_SLOT_HOLD_SECONDS)
        try:
            execute_card(c, card, src, body.path, user, fault=_FAULT_HOOK, connect=connect)
        finally:
            release_slot(c, card["source"], user)
        card = c.execute("SELECT * FROM readjudication_cards WHERE id=?", (card_id,)).fetchone()
        return {"ok": True, "card_id": card["id"], "path": card["path"], "state": card["state"]}

@app.get("/api/readjudication-cards")
def readjudication_cards(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM readjudication_cards ORDER BY id DESC").fetchall()]}

@app.get("/api/sla-policies")
def sla_policies(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM sla_policies ORDER BY id DESC").fetchall()]}

@app.get("/api/arrival-history")
def arrival_history(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM arrival_history ORDER BY id DESC").fetchall()]}

@app.get("/api/watermarks")
def watermarks(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute(
            "SELECT name, sla_minutes, tier_version, output_mode, watermark_arrival_id, watermark_at FROM sources ORDER BY id").fetchall()]}

@app.get("/api/readjudication-traces")
def readjudication_traces(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return readjudication_traces_of(c)

@app.get("/api/catchups")
def catchups(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM catchups ORDER BY id DESC").fetchall()]}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
