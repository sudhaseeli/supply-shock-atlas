"""Silver and gold: tidy the raw records, then score every port-week for abnormal traffic."""
import duckdb
import xlrd

from . import config

# Up to two years of history, skipping the latest two weeks so a shock can't hide itself. A long
# window matters: with one year a blockade becomes "normal" after six months and the alarm goes quiet.
BASELINE_WEEKS = 104
MIN_HISTORY_WEEKS = 52
GUARD_WEEKS = 2
MIN_WEEKLY_CALLS = 3      # below this a port is too quiet to score
ALARM_Z = -3.0            # robust z on the variance-stabilised count
ALARM_DROP = 0.30         # and at least 30% under expected, so wobbles at very steady ports don't fire
SURGE_Z = 3.0
OUTAGE_RATIO = 0.72       # a day this far under the global norm is a feed problem, not a world event
# Lunar New Year moves against the calendar, so these countries align those weeks on the holiday itself
LUNAR_ISO3 = ("CHN", "HKG", "TWN", "KOR", "VNM", "SGP", "MYS", "MAC")
LUNAR_NEW_YEAR = ["2019-02-05", "2020-01-25", "2021-02-12", "2022-02-01", "2023-01-22",
                  "2024-02-10", "2025-01-29", "2026-02-17", "2027-02-06"]


# Incremental runs are named from_<date> and sort newest-last; the first full load is always oldest
NEWEST_RUN = "CASE WHEN filename LIKE '%run=full%' THEN '' ELSE filename END DESC"


def connect(paths: config.Paths) -> duckdb.DuckDBPyConnection:
    paths.warehouse.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(paths.warehouse))
    for schema in ("silver", "gold", "meta"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    return con


def build_silver(con: duckdb.DuckDBPyConnection, paths: config.Paths) -> None:
    bronze = paths.bronze.as_posix()
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.ports AS
        SELECT portid, portname, country, ISO3 AS iso3, continent, lat, lon,
               vessel_count_total, vessel_count_container, vessel_count_tanker, industry_top1,
               share_country_maritime_import AS import_share, share_country_maritime_export AS export_share
        FROM read_json('{bronze}/ports.json', maximum_object_size=100000000)
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.chokepoints AS
        SELECT portid, portname AS name, lat, lon, vessel_count_total,
               vessel_count_container, vessel_count_tanker, industry_top1
        FROM read_json('{bronze}/chokepoints.json', maximum_object_size=100000000)
    """)
    # a later run revises earlier days, so keep the newest copy of each port-day
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.port_days AS
        SELECT date, portid, portcalls AS calls, portcalls_container AS container, portcalls_dry_bulk AS dry_bulk,
               portcalls_general_cargo AS general_cargo, portcalls_roro AS roro, portcalls_tanker AS tanker,
               "import" AS import_t, "export" AS export_t
        FROM read_parquet('{paths.calls.as_posix()}/run=*/*.parquet', filename=true)
        QUALIFY row_number() OVER (PARTITION BY portid, date ORDER BY {NEWEST_RUN}) = 1
    """)
    con.execute("""
        CREATE OR REPLACE TABLE silver.port_weeks AS
        WITH bounds AS (
            SELECT min(date_trunc('week', date))::DATE AS lo,
                   -- the last week only counts once all seven days are published
                   (date_trunc('week', max(date) + 1)::DATE - 7) AS hi
            FROM silver.port_days
        ), weeks AS (
            SELECT unnest(generate_series(lo, hi, INTERVAL 7 DAY))::DATE AS week FROM bounds
        ), active AS (
            SELECT date_trunc('week', date)::DATE AS week, portid, sum(calls)::INT AS calls,
                   sum(container)::INT AS container, sum(tanker)::INT AS tanker,
                   sum(import_t) AS import_t, sum(export_t) AS export_t
            FROM silver.port_days GROUP BY ALL
        )
        SELECT w.week, p.portid, coalesce(a.calls, 0) AS calls, coalesce(a.container, 0) AS container,
               coalesce(a.tanker, 0) AS tanker, coalesce(a.import_t, 0) AS import_t, coalesce(a.export_t, 0) AS export_t
        FROM weeks w CROSS JOIN (SELECT portid FROM silver.ports) p
        LEFT JOIN active a ON a.week = w.week AND a.portid = p.portid
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.events AS
        SELECT eventid, eventtype, eventname, htmlname AS title, htmldescription AS description, alertlevel, country,
               make_timestamp(fromdate * 1000)::DATE AS start_date,
               make_timestamp(todate * 1000)::DATE AS end_date,
               lat, long AS lon, affectedports, coalesce(n_affectedports, 0) AS n_affected
        FROM read_json('{bronze}/disruptions.json', maximum_object_size=100000000)
        QUALIFY row_number() OVER (PARTITION BY eventid ORDER BY editdate DESC) = 1
    """)
    con.execute("""
        CREATE OR REPLACE TABLE silver.event_ports AS
        SELECT DISTINCT eventid, trim(p) AS portid
        FROM (SELECT eventid, unnest(string_split(affectedports, ';')) AS p FROM silver.events
              WHERE affectedports IS NOT NULL AND affectedports <> '')
        WHERE trim(p) <> ''
    """)
    # the chokepoint feed has the longest outages at the source, so its absence mustn't block a run
    transit_file = paths.bronze / "chokepoint_transits.parquet"
    transit_source = (f"read_parquet('{transit_file.as_posix()}')" if transit_file.exists() else
                      """(SELECT NULL::DATE AS date, NULL::VARCHAR AS portid, NULL::VARCHAR AS portname, NULL::INT AS n_total,
                                 NULL::INT AS n_container, NULL::INT AS n_tanker, NULL::INT AS n_dry_bulk,
                                 NULL::DOUBLE AS capacity WHERE false)""")
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.transit_days AS
        SELECT date::DATE AS date, portid, portname AS name, n_total AS transits, n_container AS container,
               n_tanker AS tanker, n_dry_bulk AS dry_bulk, capacity AS tonnes
        FROM {transit_source}
    """)
    con.execute("""
        CREATE OR REPLACE TABLE silver.transit_weeks AS
        SELECT date_trunc('week', date)::DATE AS week, portid, any_value(name) AS name, sum(transits)::INT AS transits,
               sum(container)::INT AS container, sum(tanker)::INT AS tanker, sum(tonnes) AS tonnes
        FROM silver.transit_days GROUP BY ALL
        HAVING count(*) = 7
    """)
    load_gscpi(con, paths)


def load_gscpi(con: duckdb.DuckDBPyConnection, paths: config.Paths) -> None:
    sheet = xlrd.open_workbook(str(paths.bronze / "gscpi.xls")).sheet_by_name("GSCPI Monthly Data")
    rows = [(sheet.cell_value(r, 0), sheet.cell_value(r, 1)) for r in range(sheet.nrows)]
    rows = [r for r in rows if isinstance(r[1], float) and r[0]]
    con.execute("CREATE OR REPLACE TABLE silver.gscpi (month DATE, gscpi DOUBLE)")
    con.executemany("INSERT INTO silver.gscpi VALUES (date_trunc('month', strptime(?, '%d-%b-%Y'))::DATE, ?)", rows)


def build_scores(con: duckdb.DuckDBPyConnection) -> None:
    """Score each port-week against that port's own past and its normal seasonal pattern.

    Counts are compared on the 2*sqrt scale, which makes their noise roughly constant
    from a port with 5 calls a week to one with 500.
    """
    # the whole network dipping on one day means satellites or the feed, not ports
    con.execute(f"""
        CREATE OR REPLACE TABLE silver.feed_outages AS
        WITH d AS (SELECT date, sum(calls) AS n FROM silver.port_days GROUP BY date),
        m AS (SELECT *, median(n) OVER (ORDER BY date ROWS BETWEEN 28 PRECEDING AND 28 FOLLOWING) AS norm FROM d)
        SELECT date, n AS calls, norm, round(n / norm, 3) AS ratio FROM m WHERE n < norm * {OUTAGE_RATIO}
    """)
    lny = ", ".join(f"DATE '{d}'" for d in LUNAR_NEW_YEAR)
    iso = ", ".join(f"'{i}'" for i in LUNAR_ISO3)
    con.execute(f"""
        CREATE OR REPLACE TABLE gold.port_scores AS
        WITH h AS (
            SELECT week, portid, calls, container, tanker,
                   median(calls) OVER w AS baseline, mad(2 * sqrt(calls)) OVER w AS s_mad, count(*) OVER w AS n_hist
            FROM silver.port_weeks
            WINDOW w AS (PARTITION BY portid ORDER BY week
                         ROWS BETWEEN {BASELINE_WEEKS + GUARD_WEEKS - 1} PRECEDING AND {GUARD_WEEKS} PRECEDING)
        ), lunar AS (
            SELECT unnest([{lny}]) AS d
        ), keyed AS (
            SELECT h.*, p.iso3,
                   coalesce(
                       (SELECT 'lunar' || ((h.week - date_trunc('week', l.d)::DATE) / 7) FROM lunar l
                        WHERE p.iso3 IN ({iso}) AND h.week BETWEEN date_trunc('week', l.d)::DATE - 14
                                                               AND date_trunc('week', l.d)::DATE + 21 LIMIT 1),
                       -- 1 January falls in week 52, 53 or 1, so the new-year shutdown gets its own key
                       CASE WHEN year(h.week + 6) > year(h.week) THEN 'newyear' END,
                       'week' || weekofyear(h.week)) AS season
            FROM h JOIN silver.ports p USING (portid)
            WHERE n_hist >= {MIN_HISTORY_WEEKS} AND baseline >= {MIN_WEEKLY_CALLS}
        ), seasonal AS (
            -- a median across years, so one crisis year can't teach the model that a crisis is seasonal
            SELECT portid, season, least(1.5, greatest(0.4, median(calls / baseline))) AS factor, count(*) AS years
            FROM keyed GROUP BY ALL HAVING count(*) >= 3
        ), z AS (
            SELECT k.*, baseline * coalesce(s.factor, 1) AS expected
            FROM keyed k LEFT JOIN seasonal s USING (portid, season)
        ), o AS (
            SELECT date_trunc('week', date)::DATE AS week, count(*) AS outage_days FROM silver.feed_outages GROUP BY 1
        ), scored AS (
            SELECT z.*, (2 * sqrt(calls) - 2 * sqrt(expected)) / greatest(1.0, 1.4826 * s_mad) AS score,
                   coalesce(o.outage_days, 0) >= 2 AS feed_outage
            FROM z LEFT JOIN o USING (week)
        )
        SELECT week, portid, calls, container, tanker, round(expected, 2) AS baseline, round(score, 3) AS z,
               round(calls / expected - 1, 4) AS pct, feed_outage,
               NOT feed_outage AND score <= {ALARM_Z} AND calls <= expected * (1 - {ALARM_DROP}) AS alarm,
               NOT feed_outage AND score >= {SURGE_Z} AND calls >= expected * (1 + {ALARM_DROP}) AS surge
        FROM scored
    """)
    # consecutive alarm weeks at a port become one episode; a single clean week doesn't end it
    con.execute("""
        CREATE OR REPLACE TABLE gold.episodes AS
        WITH a AS (
            SELECT portid, week, baseline, calls, pct,
                   lag(week) OVER (PARTITION BY portid ORDER BY week) AS prev
            FROM gold.port_scores WHERE alarm
        ), g AS (
            SELECT *, sum(CASE WHEN prev IS NULL OR week - prev > 14 THEN 1 ELSE 0 END)
                      OVER (PARTITION BY portid ORDER BY week) AS episode
            FROM a
        )
        SELECT portid, episode, min(week) AS start_week, max(week) AS end_week,
               (max(week) - min(week)) / 7 + 1 AS weeks, count(*) AS alarm_weeks,
               round(min(pct), 3) AS worst_pct, round(sum(baseline - calls)) AS calls_lost
        FROM g GROUP BY ALL
    """)
    con.execute("""
        CREATE OR REPLACE TABLE gold.global_weekly AS
        WITH s AS (
            SELECT week, count(*) AS scored_ports, sum(calls) AS calls, sum(baseline) AS baseline,
                   sum(alarm::INT) AS alarms, sum(surge::INT) AS surges,
                   sum(CASE WHEN alarm THEN baseline ELSE 0 END) AS baseline_in_alarm
            FROM gold.port_scores GROUP BY week
        )
        SELECT week, scored_ports, calls, round(baseline) AS baseline, alarms, surges,
               round(100.0 * alarms / scored_ports, 2) AS pct_ports_in_alarm,
               -- weighted by normal traffic: one big hub going quiet counts for more than a small harbour
               round(100.0 * baseline_in_alarm / baseline, 2) AS stress_index
        FROM s ORDER BY week
    """)
