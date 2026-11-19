import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

import app as appmod
from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    monkeypatch.setattr("app._FAULT_HOOK", None)
    monkeypatch.setattr("app._SLOT_HOLD_SECONDS", 0.0)
    appmod.SESSIONS.clear()
    init_db()
    yield


def _client():
    return TestClient(app)


def _login(username="admin", password="123456"):
    c = _client()
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200, r.text
    return c, {"Authorization": "Bearer " + r.json()["token"]}


def _q(sql, *args):
    conn = sqlite3.connect(appmod.DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()


def _arrivals(source):
    return _q("SELECT * FROM arrivals WHERE source=? ORDER BY id", source)


def _source(name):
    return _q("SELECT * FROM sources WHERE name=?", name)[0]


def _change(c, h, source="支付源", path="甲", sla=60):
    return c.post("/api/readjudications", headers=h,
                  json={"source": source, "path": path, "sla_minutes": sla})


def _assert_five_places(c, h, source, new_sla, expected_level, arrival_ids):
    # 1 源目录
    s = next(x for x in c.get("/api/sources", headers=h).json()["items"] if x["name"] == source)
    assert s["sla_minutes"] == new_sla
    assert s["output_mode"] == {"正常": "可写", "告警": "降级只读", "违约": "冻结"}[expected_level]
    # 2 到达列表
    live = [x for x in c.get("/api/arrivals", headers=h).json()["items"] if x["source"] == source]
    assert {x["id"] for x in live} == set(arrival_ids)          # 不丢到达主键
    assert all(x["level"] == expected_level for x in live)
    assert all(x["adjudicated_sla"] == new_sla for x in live)
    # 3 策略附页
    pol = [x for x in c.get("/api/sla-policies", headers=h).json()["items"] if x["source"] == source]
    assert pol and pol[0]["new_sla"] == new_sla
    # 4 水位页
    wm = next(x for x in c.get("/api/watermarks", headers=h).json()["items"] if x["name"] == source)
    assert wm["watermark_arrival_id"] == max(arrival_ids)
    assert wm["watermark_at"]
    # 5 历史目录
    hist = [x for x in c.get("/api/arrival-history", headers=h).json()["items"] if x["source"] == source]
    assert hist and all(x["adjudicated_sla"] == new_sla for x in hist)
    assert {x["arrival_id"] for x in hist} == set(arrival_ids)


def test_reproduce_stale_tier_when_sla_changed_without_write_path():
    """开做前复现：改时限档后旧到达仍挂旧档，目录与历史分裂。"""
    c, h = _login()
    before = _arrivals("支付源")
    assert before and before[0]["level"] == "告警"
    conn = sqlite3.connect(appmod.DB_PATH)
    conn.execute("UPDATE sources SET sla_minutes=60 WHERE name='支付源'")
    conn.commit()
    conn.close()
    # 目录已按新档，但到达与历史仍挂旧档 —— 正是要消灭的分裂
    assert _source("支付源")["sla_minutes"] == 60
    assert _arrivals("支付源")[0]["level"] == "告警"
    assert _q("SELECT * FROM sla_policies") == []


def test_path_a_updates_all_five_places_and_keeps_arrival_ids():
    c, h = _login()
    ids = [x["id"] for x in _arrivals("支付源")]
    assert ids
    r = _change(c, h, source="支付源", path="甲", sla=60)
    assert r.status_code == 200, r.text
    _assert_five_places(c, h, "支付源", 60, "正常", ids)
    card = _q("SELECT * FROM readjudication_cards WHERE source='支付源'")[0]
    assert card["path"] == "甲" and card["frozen"] == 1 and card["state"] == "已执行"


def test_path_b_archives_snapshot_then_rechecks_under_new_tier():
    c, h = _login()
    ids = [x["id"] for x in _arrivals("物流源")]
    assert ids
    r = _change(c, h, source="物流源", path="乙", sla=200)
    assert r.status_code == 200, r.text
    _assert_five_places(c, h, "物流源", 200, "正常", ids)
    snaps = _q("SELECT * FROM arrival_history WHERE source='物流源' ORDER BY arrival_id")
    assert {x["arrival_id"] for x in snaps} == set(ids)
    assert all(x["stage"] == "已复检" for x in snaps)
    assert all(x["adjudicated_sla"] == 200 for x in snaps)


def test_path_a_midway_error_rolls_back_everything():
    """重裁失败测：甲路径半截改档必须整体回滚（半截改档须红）。"""
    c, h = _login()
    c.post("/api/arrivals", headers=h, json={
        "source": "支付源", "source_clock": "2026-11-16T09:00:00", "ingest_clock": "2026-11-16T09:35:00"})
    before_src = _source("支付源")
    before_rows = _arrivals("支付源")
    assert len(before_rows) >= 2

    calls = {"n": 0}

    def boom(stage):
        if stage == "recompute":
            calls["n"] += 1
            if calls["n"] == 2:          # 第一条重算完、第二条炸 —— 制造半截
                raise RuntimeError("injected mid-transaction")

    appmod._FAULT_HOOK = boom
    r = _change(c, h, source="支付源", path="甲", sla=60)
    assert r.status_code == 500

    after_src = _source("支付源")
    assert after_src["sla_minutes"] == before_src["sla_minutes"] == 20
    assert after_src["tier_version"] == before_src["tier_version"]
    assert after_src["output_mode"] == before_src["output_mode"] == "降级只读"
    assert after_src["watermark_arrival_id"] == before_src["watermark_arrival_id"]
    assert _arrivals("支付源") == before_rows
    assert _q("SELECT * FROM sla_policies") == []
    # 只剩失败痕，没有半截业务写入
    trails = _q("SELECT * FROM readjudication_audit")
    assert [t["action"] for t in trails] == ["回滚"]
    card = _q("SELECT * FROM readjudication_cards WHERE source='支付源'")[0]
    assert card["state"] == "执行失败" and card["frozen"] == 1


def test_path_b_interrupt_keeps_snapshot_and_output_state_unchanged():
    """乙路径中断：快照保留、输出态不动、档位不半改。"""
    c, h = _login()
    before_src = _source("支付源")
    before_rows = _arrivals("支付源")

    def boom(stage):
        if stage == "recheck":
            raise RuntimeError("injected during recheck")

    appmod._FAULT_HOOK = boom
    r = _change(c, h, source="支付源", path="乙", sla=60)
    assert r.status_code == 500

    after_src = _source("支付源")
    assert after_src["sla_minutes"] == 20
    assert after_src["output_mode"] == "降级只读"
    assert _arrivals("支付源") == before_rows
    snaps = _q("SELECT * FROM arrival_history WHERE source='支付源'")
    assert {x["arrival_id"] for x in snaps} == {x["id"] for x in before_rows}
    assert all(x["stage"] == "待复检" for x in snaps)
    assert _q("SELECT * FROM sla_policies") == []


def test_verdict_card_is_frozen_and_cannot_be_replaced_same_day():
    c, h = _login()
    assert _change(c, h, source="支付源", path="甲", sla=60).status_code == 200
    r = _change(c, h, source="支付源", path="甲", sla=90)
    assert r.status_code == 409
    cards = _q("SELECT * FROM readjudication_cards WHERE source='支付源'")
    assert len(cards) == 1 and cards[0]["new_sla"] == 60 and cards[0]["frozen"] == 1


def test_deviation_from_verdict_card_is_rejected_and_traced():
    c, h = _login()
    appmod._FAULT_HOOK = lambda stage: (_ for _ in ()).throw(RuntimeError("fail once"))
    assert _change(c, h, source="支付源", path="甲", sla=60).status_code == 500
    appmod._FAULT_HOOK = None
    card = _q("SELECT * FROM readjudication_cards WHERE source='支付源'")[0]
    r = c.post(f"/api/readjudications/{card['id']}/execute", headers=h, json={"path": "乙"})
    assert r.status_code == 422 and "重裁偏了" in r.json()["detail"]
    assert _source("支付源")["sla_minutes"] == 20
    dev = _q("SELECT * FROM readjudication_deviations WHERE card_id=?", card["id"])
    assert dev and "重裁偏了" in dev[0]["detail"]
    # 冻结后仍按原卡原路径执行
    r = c.post(f"/api/readjudications/{card['id']}/execute", headers=h, json={"path": "甲"})
    assert r.status_code == 200 and _source("支付源")["sla_minutes"] == 60


def test_two_sided_concurrent_change_holds_slot_and_leaves_contention_trace():
    """双端抢改失败测：配置员与补采员抢同一源，后手只留重裁争用痕。"""
    monkey = pytest.MonkeyPatch()
    monkey.setattr("app._SLOT_HOLD_SECONDS", 0.6)
    try:
        cc, hc = _login("admin", "123456")          # 配置员
        co, ho = _login("ops", "123456")            # 补采员

        def config_side():
            cl = _client()
            return ("config", cl.post("/api/readjudications", headers=hc,
                                      json={"source": "支付源", "path": "甲", "sla_minutes": 60}))

        def catchup_side():
            cl = _client()
            return ("catchup", cl.post("/api/catchup/支付源", headers=ho))

        with ThreadPoolExecutor(max_workers=2) as ex:
            results = [f.result() for f in [ex.submit(config_side), ex.submit(catchup_side)]]
    finally:
        monkey.undo()

    codes = {who: r.status_code for who, r in results}
    assert sorted(codes.values()) == [200, 409], codes
    loser = next(who for who, r in results if r.status_code == 409)
    if loser == "catchup":
        assert _q("SELECT * FROM catchups") == []
        assert _source("支付源")["sla_minutes"] == 60   # 配置侧已落地
    else:
        assert _q("SELECT * FROM readjudication_cards") == []
        assert _source("支付源")["sla_minutes"] == 20
    traces = _q("SELECT * FROM readjudication_contention WHERE source='支付源'")
    assert traces, "缺重裁争用留痕"


def test_sla_change_is_impossible_without_re_adjudication():
    c, h = _login()
    r = c.post("/api/readjudications", headers=h, json={"source": "支付源", "sla_minutes": 60})
    assert r.status_code == 422                    # 无路径 = 不予通过
    assert _source("支付源")["sla_minutes"] == 20
    assert c.post("/api/sources/支付源/sla", headers=h, json={"sla_minutes": 60}).status_code in (404, 405)


def test_roles_are_separated_for_config_and_catchup():
    _, hc = _login("admin", "123456")
    co, ho = _login("ops", "123456")
    cv, hv = _login("viewer", "view123")
    assert _change(co, ho, source="支付源", path="甲", sla=60).status_code == 403
    assert co.post("/api/catchup/支付源", headers=ho).status_code == 200
    assert cv.post("/api/readjudications", headers=hv,
                   json={"source": "物流源", "path": "甲", "sla_minutes": 60}).status_code == 403


def test_acceptance_five_places_readable_under_new_tier_both_paths():
    """验收：甲/乙各自落地后，源目录、到达列表、策略附页、水位页、历史目录五处均按新档可读。"""
    c, h = _login()
    c.post("/api/arrivals", headers=h, json={
        "source": "订单源", "source_clock": "2026-11-16T10:00:00", "ingest_clock": "2026-11-16T10:20:00"})
    for source, path, new_sla in (("订单源", "甲", 45), ("物流源", "乙", 200)):
        assert _arrivals(source), source
        r = _change(c, h, source=source, path=path, sla=new_sla)
        assert r.status_code == 200, (source, r.text)
        s = next(x for x in c.get("/api/sources", headers=h).json()["items"] if x["name"] == source)
        assert s["sla_minutes"] == new_sla
        live = [x for x in c.get("/api/arrivals", headers=h).json()["items"] if x["source"] == source]
        assert live and all(x["adjudicated_sla"] == new_sla for x in live)
        assert {x["id"] for x in live} == set(x["id"] for x in _arrivals(source))
        assert [p for p in c.get("/api/sla-policies", headers=h).json()["items"] if p["source"] == source]
        w = next(x for x in c.get("/api/watermarks", headers=h).json()["items"] if x["name"] == source)
        assert w["sla_minutes"] == new_sla and w["watermark_arrival_id"] == max(x["id"] for x in live)
        hist = [x for x in c.get("/api/arrival-history", headers=h).json()["items"] if x["source"] == source]
        assert hist and all(x["adjudicated_sla"] == new_sla for x in hist)


def test_page_serves_tier_policy_tab_not_a_new_readjudication_desk():
    c = _client()
    html = c.get("/").text
    assert "时限台策略" in html
    assert "水位页" in html and "历史目录" in html
    assert "重裁台" not in html
