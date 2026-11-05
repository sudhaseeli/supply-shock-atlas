"""Data-quality gate: a critical failure stops the run, so nothing wrong reaches the dashboard."""
import json
from dataclasses import asdict, dataclass

import duckdb

from . import config, transform


@dataclass
class Check:
    name: str
    severity: str
    passed: bool
    detail: str


def value(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def run_checks(con: duckdb.DuckDBPyConnection, paths: config.Paths, log=print) -> tuple[bool, list[Check]]:
    results: list[Check] = []

    def check(name: str, severity: str, passed: bool, detail: str) -> None:
        results.append(Check(name, severity, bool(passed), detail))

    days = value(con, "SELECT count(*) FROM silver.port_days")
    check("port_days_not_empty", "critical", days > 0, f"{days:,} active port-days")

    dupes = value(con, "SELECT count(*) FROM (SELECT 1 FROM silver.port_days GROUP BY portid, date HAVING count(*) > 1)")
    check("one_row_per_port_day", "critical", dupes == 0, f"{dupes} duplicate port-days")

    # the raw shards and the week table must hold exactly the same number of calls
    raw = value(con, f"""SELECT sum(portcalls) FROM (
        SELECT portid, date, portcalls FROM read_parquet('{paths.calls.as_posix()}/run=*/*.parquet', filename=true)
        QUALIFY row_number() OVER (PARTITION BY portid, date ORDER BY {transform.NEWEST_RUN}) = 1)""")
    last_full_week = value(con, "SELECT max(week) FROM silver.port_weeks")
    in_silver = value(con, "SELECT sum(calls) FROM silver.port_days")
    trailing = value(con, f"SELECT coalesce(sum(calls), 0) FROM silver.port_days WHERE date >= DATE '{last_full_week}' + 7")
    weekly = value(con, "SELECT sum(calls) FROM silver.port_weeks")
    check("calls_reconcile_bronze_to_silver", "critical", raw == in_silver, f"bronze {raw:,} = silver {in_silver:,}")
    check("calls_reconcile_days_to_weeks", "critical", in_silver == weekly + trailing,
          f"days {in_silver:,} = weeks {weekly:,} + unfinished week {trailing:,}")

    bad_types = value(con, """SELECT count(*) FROM silver.port_days
                              WHERE container + dry_bulk + general_cargo + roro + tanker <> calls""")
    check("vessel_types_sum_to_total", "critical", bad_types == 0, f"{bad_types} port-days where types don't sum")

    negative = value(con, "SELECT count(*) FROM silver.port_days WHERE calls < 0 OR import_t < 0 OR export_t < 0")
    check("no_negative_values", "critical", negative == 0, f"{negative} negative values")

    orphans = value(con, "SELECT count(DISTINCT portid) FROM silver.port_days WHERE portid NOT IN (SELECT portid FROM silver.ports)")
    check("every_port_in_reference", "critical", orphans == 0, f"{orphans} unknown port IDs")

    event_orphans = value(con, "SELECT count(*) FROM silver.event_ports WHERE portid NOT LIKE 'chokepoint%' AND portid NOT IN (SELECT portid FROM silver.ports)")
    check("event_ports_in_reference", "warn", event_orphans == 0, f"{event_orphans} logged event-ports not in the reference")

    panel = value(con, """SELECT count(*) FROM (SELECT portid FROM silver.port_weeks GROUP BY portid
                          HAVING count(*) <> (SELECT count(DISTINCT week) FROM silver.port_weeks))""")
    check("week_panel_complete", "critical", panel == 0, f"{panel} ports with missing weeks")

    gaps = value(con, """SELECT count(*) FROM (
        SELECT unnest(generate_series((SELECT min(date) FROM silver.port_days), (SELECT max(date) FROM silver.port_days),
                                      INTERVAL 1 DAY))::DATE AS d)
        WHERE d NOT IN (SELECT date FROM silver.port_days)""")
    check("no_missing_calendar_days", "critical", gaps == 0, f"{gaps} days with no records at all")

    score_gap = value(con, """SELECT abs((SELECT sum(calls) FROM gold.port_scores)
                                        - (SELECT sum(calls) FROM gold.global_weekly))""")
    check("gold_reconciles_to_scores", "critical", score_gap == 0, f"difference {score_gap}")

    alarm_rate = value(con, "SELECT round(100.0 * avg(alarm::INT), 2) FROM gold.port_scores")
    check("alarm_rate_plausible", "warn", 0.2 <= alarm_rate <= 8, f"{alarm_rate}% of port-weeks in alarm")

    stale = value(con, "SELECT current_date - max(date) FROM silver.port_days")
    check("data_is_fresh", "warn", stale <= 21, f"latest data is {stale} days old")

    outages = value(con, "SELECT count(*) FROM silver.feed_outages")
    check("feed_outages_low", "warn", outages <= 60, f"{outages} days flagged as feed outages")

    transit_days = value(con, "SELECT count(*) FROM (SELECT 1 FROM silver.transit_days GROUP BY portid, date HAVING count(*) > 1)")
    check("one_row_per_transit_day", "critical", transit_days == 0, f"{transit_days} duplicate chokepoint-days")

    transit_rows = value(con, "SELECT count(*) FROM silver.transit_days")
    check("chokepoint_feed_present", "warn", transit_rows > 0, f"{transit_rows:,} chokepoint-days")

    ok = all(r.passed for r in results if r.severity == "critical")
    paths.reports.mkdir(parents=True, exist_ok=True)
    (paths.reports / "dq_report.json").write_text(json.dumps(
        {"passed": ok, "checks": [asdict(r) for r in results]}, indent=2))
    for r in results:
        log(f"  {'pass' if r.passed else 'FAIL'}  {r.severity:<8} {r.name}: {r.detail}")
    return ok, results
