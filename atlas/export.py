"""Gold to docs/data/atlas.json, the only file the dashboard reads."""
import json
from pathlib import Path

import duckdb

from . import analysis, config, transform

MAP_PORTS = 700         # busiest scored ports drawn on the animated map
SIDE_ROWS = 12          # winners and losers per crisis


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    return [{n: (str(v) if hasattr(v, "isoformat") else v) for n, v in zip(names, row)} for row in cur.fetchall()]


def map_frames(con: duckdb.DuckDBPyConnection) -> dict:
    """One compact string per week: each port's traffic vs expected, as one character per port.

    Characters 0-8 hold a bucket from 'far below' to 'far above'; '.' means not scored that week.
    Every week of 700 ports therefore costs 700 bytes, which gzip shrinks further.
    """
    ports = rows(con, f"""
        SELECT p.portid AS id, p.portname AS name, p.country, round(p.lat, 2) AS lat, round(p.lon, 2) AS lon,
               round(median(s.baseline), 1) AS normal
        FROM gold.port_scores s JOIN silver.ports p USING (portid)
        GROUP BY ALL ORDER BY normal DESC, id LIMIT {MAP_PORTS}
    """)
    index = {p["id"]: i for i, p in enumerate(ports)}
    weeks = [r[0] for r in con.execute("SELECT DISTINCT week FROM gold.port_scores ORDER BY week").fetchall()]
    week_index = {w: i for i, w in enumerate(weeks)}
    grid = [["."] * len(ports) for _ in weeks]
    for week, portid, pct, alarm, surge in con.execute(
            "SELECT week, portid, pct, alarm, surge FROM gold.port_scores").fetchall():
        i = index.get(portid)
        if i is None:
            continue
        # only confirmed alarms and surges reach the outer buckets, so colour always means significance
        if alarm:
            bucket = 0 if pct <= -0.7 else 1 if pct <= -0.5 else 2
        elif surge:
            bucket = 8 if pct >= 1 else 7 if pct >= 0.6 else 6
        else:
            bucket = 3 if pct < -0.1 else 5 if pct > 0.1 else 4
        grid[week_index[week]][i] = str(bucket)
    return {"ports": ports, "weeks": [str(w) for w in weeks], "frames": ["".join(g) for g in grid]}


def crisis_block(con: duckdb.DuckDBPyConnection, crisis: dict) -> dict:
    cid = crisis["id"]
    table = f"""SELECT c.portid, p.portname AS name, p.country, c.calls_before, c.calls_now, c.change_pct, c.delta
                FROM gold.crisis_ports c JOIN silver.ports p USING (portid) WHERE c.crisis = '{cid}'"""
    totals = f"""SELECT sum(calls_before) AS before, sum(calls_now) AS now,
                        round(100 * (sum(calls_now) / sum(calls_before) - 1), 1) AS change_pct, count(*) AS ports
                 FROM gold.crisis_ports WHERE crisis = '{cid}'"""
    return {
        **{k: crisis[k] for k in ("id", "name", "start", "end", "zone_name", "alt_name")},
        "zone_total": rows(con, f"{totals} AND role = 'zone'")[0],
        "alt_total": rows(con, f"{totals} AND role = 'alt'")[0],
        "losers": rows(con, f"{table} AND c.role = 'zone' ORDER BY delta LIMIT {SIDE_ROWS}"),
        "winners": rows(con, f"{table} AND c.role = 'alt' ORDER BY delta DESC LIMIT {SIDE_ROWS}"),
        "alt_losers": rows(con, f"{table} AND c.role = 'alt' ORDER BY delta LIMIT 5"),
        "countries": rows(con, f"""SELECT p.country, count(*) AS ports, round(sum(c.calls_before)) AS before, round(sum(c.calls_now)) AS now,
                                          round(100 * (sum(c.calls_now) / sum(c.calls_before) - 1), 1) AS change_pct
                                   FROM gold.crisis_ports c JOIN silver.ports p USING (portid)
                                   WHERE c.crisis = '{cid}' AND c.role = 'zone' GROUP BY p.country ORDER BY before DESC LIMIT 10"""),
        "chokepoints": rows(con, f"""SELECT portid, name, transits_before, transits_now, change_pct
                                     FROM gold.crisis_chokepoints WHERE crisis = '{cid}'"""),
        "series": rows(con, f"""SELECT series, week, idx FROM gold.crisis_series WHERE crisis = '{cid}'
                                AND week BETWEEN DATE '{crisis['start']}' - 364 AND DATE '{crisis['start']}' + 560
                                ORDER BY week"""),
    }


def export(paths: config.Paths, out: Path = Path("docs/data/atlas.json")) -> Path:
    con = duckdb.connect(str(paths.warehouse), read_only=True)
    try:
        data = {
            "meta": rows(con, """SELECT (SELECT max(date) FROM silver.port_days) AS last_day,
                                        (SELECT max(week) FROM silver.port_weeks) AS last_week,
                                        (SELECT count(DISTINCT portid) FROM silver.port_days) AS ports,
                                        (SELECT count(DISTINCT p.iso3) FROM silver.ports p) AS countries,
                                        (SELECT sum(calls) FROM silver.port_days) AS port_calls,
                                        (SELECT count(*) FROM silver.port_days) AS port_days,
                                        (SELECT count(*) FROM gold.port_scores) AS scored_port_weeks,
                                        (SELECT sum(alarm::INT) FROM gold.port_scores) AS alarms,
                                        (SELECT count(*) FROM gold.episodes) AS episodes,
                                        (SELECT count(*) FROM silver.feed_outages) AS outage_days""")[0],
            "map": map_frames(con),
            "chokepoints": rows(con, """SELECT portid AS id, name, round(lat, 2) AS lat, round(lon, 2) AS lon,
                                               vessel_count_total AS vessels FROM silver.chokepoints"""),
            "stress": rows(con, "SELECT week, stress_index, pct_ports_in_alarm, alarms, surges, scored_ports FROM gold.global_weekly"),
            "events": rows(con, """SELECT eventname AS name, eventtype AS type, country, start_date AS start, end_date AS "end", n_affected
                                   FROM silver.events WHERE n_affected >= 5 OR eventtype = 'OT' ORDER BY start_date"""),
                        "crises": [crisis_block(con, c) for c in analysis.CRISES],
            "backtest": rows(con, "SELECT * FROM gold.backtest_summary"),
            "backtest_named": rows(con, """SELECT eventname AS name, eventtype AS type, start_date AS start,
                                                  count(*) AS ports, sum(hit::INT) AS detected
                                           FROM gold.backtest WHERE eventtype = 'OT' GROUP BY ALL ORDER BY start_date"""),
            "recovery": rows(con, """SELECT eventtype AS type, count(*) AS port_events,
                                            median(weeks_to_recover) AS median_weeks,
                                            quantile_cont(weeks_to_recover, 0.9) AS p90_weeks,
                                            sum((weeks_to_recover IS NULL)::INT) AS not_recovered
                                     FROM gold.recovery GROUP BY 1 ORDER BY port_events DESC"""),
            "longest": rows(con, f"""SELECT p.portname AS name, p.country, e.start_week, e.end_week, e.weeks, e.worst_pct, e.calls_lost, v.evidence
                                     FROM gold.episodes e JOIN silver.ports p USING (portid)
                                     JOIN gold.episode_evidence v USING (portid, episode)
                                     ORDER BY e.calls_lost DESC LIMIT 15"""),
            "evidence": rows(con, """SELECT evidence, count(*) AS n FROM gold.episode_evidence v JOIN gold.episodes e USING (portid, episode)
                                     WHERE e.weeks >= 4 GROUP BY 1 ORDER BY n DESC"""),
            "exposure": rows(con, """SELECT iso3, country, ports, weekly_calls, top_port, top_port_import_share, hhi
                                     FROM gold.country_exposure
                                     WHERE weekly_calls >= 15 AND top_port_import_share >= 60 AND country NOT LIKE '% SAR'
                                     ORDER BY weekly_calls DESC"""),
            "pressure": rows(con, "SELECT month, round(gscpi, 3) AS gscpi, round(stress, 2) AS stress FROM gold.monthly_pressure"),
            "pressure_lags": rows(con, "SELECT * FROM gold.pressure_lags"),
            "feed_outages": rows(con, "SELECT date, ratio FROM silver.feed_outages ORDER BY date"),
            "data_quality": json.loads((paths.reports / "dq_report.json").read_text()),
            "params": {"baseline_weeks": transform.BASELINE_WEEKS, "alarm_z": transform.ALARM_Z, "alarm_drop": transform.ALARM_DROP},
        }
    finally:
        con.close()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, separators=(",", ":"), default=float))
    return out
