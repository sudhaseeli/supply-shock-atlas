import json
from datetime import timedelta

import duckdb

from atlas import export, ingest, quality
from atlas.cli import build

from conftest import QUIET_SHOCK_END, SHOCK_WEEK, START, WEEKS

SHOCK_START = START + timedelta(weeks=SHOCK_WEEK)
QUIET = lambda *_: None


def built(paths):
    assert build(paths, log=QUIET)
    return duckdb.connect(str(paths.warehouse))


def test_weeks_rebuild_zero_days_and_reconcile(paths):
    con = built(paths)
    assert con.execute("SELECT count(DISTINCT week) FROM silver.port_weeks").fetchone()[0] == WEEKS
    # TINY never reported traffic but still has a full, all-zero panel
    assert con.execute("SELECT count(*), sum(calls) FROM silver.port_weeks WHERE portid = 'TINY'").fetchone() == (WEEKS, 0)
    dark = con.execute("SELECT calls FROM silver.port_weeks WHERE portid = 'HUB' AND week = ?", [SHOCK_START]).fetchone()[0]
    assert dark == 0


def test_blockade_fires_an_alarm_and_a_steady_port_does_not(paths):
    con = built(paths)
    first, weeks, worst = con.execute("""SELECT start_week, weeks, worst_pct FROM gold.episodes
                                         WHERE portid = 'HUB' ORDER BY calls_lost DESC""").fetchone()
    assert first == SHOCK_START
    assert weeks == QUIET_SHOCK_END - SHOCK_WEEK
    assert worst == -1
    assert con.execute("SELECT sum(alarm::INT) FROM gold.port_scores WHERE portid = 'STEADY'").fetchone()[0] == 0


def test_quiet_ports_are_not_scored(paths):
    con = built(paths)
    assert con.execute("SELECT count(*) FROM gold.port_scores WHERE portid = 'TINY'").fetchone()[0] == 0


def test_annual_shutdown_is_learned_as_seasonal_not_a_shock(paths):
    con = built(paths)
    alarms = con.execute("SELECT count(*) FROM gold.port_scores WHERE portid = 'SEASONAL' AND alarm").fetchone()[0]
    assert alarms == 0


def test_network_wide_dip_is_a_feed_outage_not_an_alarm(paths):
    for port in ("HUB", "SEASONAL", "STEADY"):
        shard = (paths.calls / "run=full" / f"{port}.parquet").as_posix()
        con = duckdb.connect()
        # every port reports almost nothing for five days of the same week
        lo = START + timedelta(weeks=120)
        hi = lo + timedelta(days=4)
        low = f"date BETWEEN DATE '{lo}' AND DATE '{hi}'"
        con.execute(f"""COPY (SELECT * REPLACE (CASE WHEN {low} THEN 0 ELSE portcalls_container END AS portcalls_container,
                                                CASE WHEN {low} THEN 1 ELSE portcalls END AS portcalls)
                              FROM read_parquet('{shard}')) TO '{shard}.tmp' (FORMAT parquet)""")
        con.close()
        (paths.calls / "run=full" / f"{port}.parquet.tmp").replace(shard)
    con = built(paths)
    assert con.execute("SELECT count(*) FROM silver.feed_outages").fetchone()[0] >= 3
    flagged = con.execute("SELECT bool_or(feed_outage), sum(alarm::INT) FROM gold.port_scores WHERE week = ?",
                          [START + timedelta(weeks=120)]).fetchone()
    assert flagged == (True, 0)


def test_revised_days_replace_older_copies(paths):
    newer = paths.calls / "run=from_2025"
    newer.mkdir()
    day = START + timedelta(days=3)
    con = duckdb.connect()
    con.execute(f"""COPY (SELECT DATE '{day}' AS date, 'HUB' AS portid, 98 AS portcalls_container, 0 AS portcalls_dry_bulk,
                                 0 AS portcalls_general_cargo, 0 AS portcalls_roro, 1 AS portcalls_tanker, 99 AS portcalls,
                                 1.0::DOUBLE AS "import", 1.0::DOUBLE AS "export") TO '{(newer / 'HUB.parquet').as_posix()}' (FORMAT parquet)""")
    con.close()
    con = built(paths)
    assert con.execute("SELECT calls FROM silver.port_days WHERE portid = 'HUB' AND date = ?", [day]).fetchall() == [(99,)]


def test_backtest_credits_the_logged_event(paths):
    con = built(paths)
    assert con.execute("SELECT eventname, hit, placebo_hit FROM gold.backtest").fetchall() == [("TEST BLOCKADE", True, False)]
    row = con.execute("SELECT port_events, detected, detection_pct FROM gold.backtest_summary").fetchone()
    assert row == (1, 1, 100.0)


def test_recovery_time_is_measured_from_first_alarm(paths):
    con = built(paths)
    weeks = con.execute("SELECT weeks_to_recover FROM gold.recovery WHERE portid = 'HUB'").fetchone()[0]
    assert weeks == QUIET_SHOCK_END - SHOCK_WEEK


def test_episodes_are_graded_by_evidence(paths):
    con = built(paths)
    grades = con.execute("""SELECT v.evidence FROM gold.episode_evidence v JOIN gold.episodes e USING (portid, episode)
                             WHERE v.portid = 'HUB' AND e.start_week = ?""", [SHOCK_START]).fetchall()
    assert grades == [("In the IMF log",)]


def test_quality_gate_catches_corruption(paths):
    con = built(paths)
    con.execute("UPDATE silver.port_days SET calls = calls + 1 WHERE portid = 'HUB' AND date = ?", [START])
    con.execute("INSERT INTO silver.port_days SELECT * FROM silver.port_days LIMIT 1")
    ok, results = quality.run_checks(con, paths, log=QUIET)
    failed = {r.name for r in results if not r.passed}
    assert not ok
    assert {"one_row_per_port_day", "calls_reconcile_bronze_to_silver", "vessel_types_sum_to_total"} <= failed


def test_export_writes_map_frames_and_marts(paths, tmp_path):
    built(paths).close()
    data = json.loads(export.export(paths, tmp_path / "atlas.json").read_text())
    atlas = data["map"]
    assert len(atlas["frames"]) == len(atlas["weeks"])
    assert all(len(f) == len(atlas["ports"]) for f in atlas["frames"])
    hub = next(i for i, p in enumerate(atlas["ports"]) if p["id"] == "HUB")
    assert atlas["frames"][atlas["weeks"].index(str(SHOCK_START))][hub] == "0"
    assert data["data_quality"]["passed"] is True


def test_ingest_keeps_a_complete_shard_and_rejects_a_short_download(paths, monkeypatch):
    calls = []

    def fake_query(url, **params):
        calls.append(params)
        return {"count": 2}

    monkeypatch.setattr(ingest, "query", fake_query)
    monkeypatch.setattr(ingest, "fetch_all", lambda *a, **k: [{"portcalls": 1}])  # one row short, every attempt
    try:
        ingest.load_port(paths, "probe", "NEWPORT", "portcalls>0")
        raise AssertionError("short download should raise")
    except RuntimeError as e:
        assert "server says 2" in str(e)
    assert not list(paths.calls.glob("run=probe/*.parquet"))
