"""Gold marts that answer questions: does the detector work, what did each crisis do, who is exposed?"""
import duckdb

# Backtest: an alarm counts if it starts within this window around the logged event
HIT_WEEKS_BEFORE = 1
HIT_WEEKS_AFTER = 3

# Each crisis is compared with the same weeks one year earlier, which removes seasonality.
# `zone` is where the shock landed and `alternatives` is where traffic could have moved to,
# as (lat_min, lat_max, lon_min, lon_max) boxes. Ranking the whole world instead just
# surfaces big ports with unrelated trends.
CRISES = [
    {"id": "hormuz", "name": "Strait of Hormuz", "start": "2026-03-02", "end": None,
     "zone_name": "Inside the Persian Gulf", "zone": [(23.5, 31, 47, 56.3)],
     "alt_name": "Gulf of Oman, Arabian Sea and Red Sea", "alternatives": [(5, 28, 56.3, 75), (12, 30, 32, 44)],
     "chokepoints": ["chokepoint6"]},
    {"id": "redsea", "name": "Red Sea attacks", "start": "2023-12-18", "end": "2024-12-16",
     "zone_name": "Red Sea and Suez", "zone": [(11, 31.5, 32, 44)],
     "alt_name": "Around Africa", "alternatives": [(-40, 30, -25, 20), (-40, -5, 20, 60)],
     "chokepoints": ["chokepoint4", "chokepoint1", "chokepoint7"]},
    {"id": "panama", "name": "Panama Canal drought", "start": "2023-11-06", "end": "2024-08-05",
     "zone_name": "Panama", "zone": [(7, 10, -83, -77)],
     "alt_name": "Rest of the Americas", "alternatives": [(-56, 7, -90, -30), (10, 50, -130, -60)],
     "chokepoints": ["chokepoint2", "chokepoint21"]},
    {"id": "baltimore", "name": "Baltimore bridge collapse", "start": "2024-03-25", "end": "2024-06-10",
     "zone_name": "Baltimore", "zone": [(38.8, 39.6, -77, -76)],
     "alt_name": "Other US East Coast ports", "alternatives": [(25, 38.8, -82, -66), (39.6, 45, -82, -66)],
     "chokepoints": []},
]
PRIOR_DAYS = 364
INDEX_WEEKS = 26          # the series are indexed to the average of this many weeks before the crisis
NEIGHBOUR_KM = 500


def in_boxes(boxes: list[tuple]) -> str:
    return " OR ".join(f"(lat BETWEEN {a} AND {b} AND lon BETWEEN {c} AND {d})" for a, b, c, d in boxes)


def build_backtest(con: duckdb.DuckDBPyConnection) -> None:
    """Did an alarm fire at the ports the IMF logged as hit, and how often would one fire by chance?"""
    con.execute(f"""
        CREATE OR REPLACE TABLE gold.backtest AS
        WITH targets AS (
            SELECT e.eventid, e.eventtype, e.eventname, e.start_date,
                   coalesce(e.end_date, e.start_date) AS end_date, ep.portid
            FROM silver.events e JOIN silver.event_ports ep USING (eventid)
            WHERE e.start_date >= (SELECT min(week) FROM gold.port_scores)
        ), scored AS (
            -- only port-events the detector could have seen: the port was being scored that week
            SELECT t.* FROM targets t
            WHERE EXISTS (SELECT 1 FROM gold.port_scores s WHERE s.portid = t.portid
                          AND s.week BETWEEN date_trunc('week', t.start_date)::DATE
                                         AND date_trunc('week', t.end_date)::DATE + {7 * HIT_WEEKS_AFTER})
        ), joined AS (
            SELECT t.*,
                   EXISTS (SELECT 1 FROM gold.port_scores s WHERE s.portid = t.portid AND s.alarm
                           AND s.week BETWEEN date_trunc('week', t.start_date)::DATE - {7 * HIT_WEEKS_BEFORE}
                                          AND date_trunc('week', t.end_date)::DATE + {7 * HIT_WEEKS_AFTER}) AS hit,
                   -- placebo: same port and same length of window, one year earlier
                   EXISTS (SELECT 1 FROM gold.port_scores s WHERE s.portid = t.portid AND s.alarm
                           AND s.week BETWEEN date_trunc('week', t.start_date)::DATE - 364 - {7 * HIT_WEEKS_BEFORE}
                                          AND date_trunc('week', t.end_date)::DATE - 364 + {7 * HIT_WEEKS_AFTER}) AS placebo_hit,
                   EXISTS (SELECT 1 FROM gold.port_scores s WHERE s.portid = t.portid
                           AND s.week BETWEEN date_trunc('week', t.start_date)::DATE - 364 - {7 * HIT_WEEKS_BEFORE}
                                          AND date_trunc('week', t.end_date)::DATE - 364 + {7 * HIT_WEEKS_AFTER}) AS placebo_scored
            FROM scored t
        )
        SELECT * FROM joined
    """)
    con.execute("""
        CREATE OR REPLACE TABLE gold.backtest_summary AS
        SELECT CASE eventtype WHEN 'TC' THEN 'Tropical cyclone' WHEN 'EQ' THEN 'Earthquake' WHEN 'FL' THEN 'Flood'
                    WHEN 'OT' THEN 'Conflict, strike, accident' WHEN 'DR' THEN 'Drought' WHEN 'WF' THEN 'Wildfire'
                    WHEN 'VO' THEN 'Volcano' ELSE eventtype END AS event_type,
               count(*) AS port_events, count(DISTINCT eventid) AS events, sum(hit::INT) AS detected,
               round(100.0 * avg(hit::INT), 1) AS detection_pct,
               round(100.0 * sum(CASE WHEN placebo_scored AND placebo_hit THEN 1 ELSE 0 END)
                     / nullif(sum(placebo_scored::INT), 0), 1) AS placebo_pct
        FROM gold.backtest GROUP BY 1 ORDER BY port_events DESC
    """)


def build_recovery(con: duckdb.DuckDBPyConnection) -> None:
    """How long hit ports took to get back to 90% of normal, by what hit them."""
    con.execute("""
        CREATE OR REPLACE TABLE gold.recovery AS
        WITH matched AS (
            SELECT b.eventid, b.eventtype, b.portid, b.start_date
            FROM gold.backtest b WHERE b.hit
        ), first_alarm AS (
            SELECT m.eventid, m.eventtype, m.portid, min(s.week) AS alarm_week
            FROM matched m JOIN gold.port_scores s ON s.portid = m.portid AND s.alarm
                 AND s.week >= date_trunc('week', m.start_date)::DATE - 7
            GROUP BY ALL
        ), rec AS (
            SELECT f.eventid, f.eventtype, f.portid, f.alarm_week,
                   min(s.week) FILTER (WHERE s.week > f.alarm_week AND s.pct >= -0.10) AS back_week
            FROM first_alarm f JOIN gold.port_scores s ON s.portid = f.portid
            GROUP BY ALL
        )
        SELECT *, (back_week - alarm_week) / 7 AS weeks_to_recover FROM rec
    """)


def build_crises(con: duckdb.DuckDBPyConnection) -> None:
    last_week = con.execute("SELECT max(week) FROM silver.port_weeks").fetchone()[0]
    con.execute("""CREATE OR REPLACE TABLE gold.crisis_ports (crisis VARCHAR, role VARCHAR, portid VARCHAR, calls_now DOUBLE,
                   calls_before DOUBLE, change_pct DOUBLE, delta DOUBLE)""")
    con.execute("""CREATE OR REPLACE TABLE gold.crisis_chokepoints (crisis VARCHAR, portid VARCHAR, name VARCHAR,
                   transits_now DOUBLE, transits_before DOUBLE, change_pct DOUBLE)""")
    con.execute("CREATE OR REPLACE TABLE gold.crisis_series (crisis VARCHAR, series VARCHAR, week DATE, idx DOUBLE)")
    for c in CRISES:
        end = c["end"] or str(last_week)
        window = f"BETWEEN DATE '{c['start']}' AND DATE '{end}'"
        prior = f"BETWEEN DATE '{c['start']}' - {PRIOR_DAYS} AND DATE '{end}' - {PRIOR_DAYS}"
        for role, boxes, label in (("zone", c["zone"], c["zone_name"]), ("alt", c["alternatives"], c["alt_name"])):
            ports = f"(SELECT portid FROM silver.ports WHERE {in_boxes(boxes)})"
            con.execute(f"""
                INSERT INTO gold.crisis_ports
                WITH now AS (SELECT portid, avg(calls) AS v FROM silver.port_weeks
                             WHERE week {window} AND portid IN {ports} GROUP BY portid),
                     was AS (SELECT portid, avg(calls) AS v FROM silver.port_weeks
                             WHERE week {prior} AND portid IN {ports} GROUP BY portid)
                SELECT '{c["id"]}', '{role}', n.portid, round(n.v, 2), round(w.v, 2),
                       round(100 * (n.v / w.v - 1), 1), round(n.v - w.v, 2)
                FROM now n JOIN was w USING (portid) WHERE w.v >= 3
            """)
            con.execute(f"""
                INSERT INTO gold.crisis_series
                WITH w AS (SELECT week, sum(calls) AS n FROM silver.port_weeks WHERE portid IN {ports} GROUP BY week),
                base AS (SELECT avg(n) AS b FROM w WHERE week < DATE '{c['start']}'
                         AND week >= DATE '{c['start']}' - {7 * INDEX_WEEKS})
                SELECT '{c["id"]}', '{label}', week, round(100 * n / b, 1) FROM w, base WHERE b > 0
            """)
        if not c["chokepoints"]:
            continue
        ids = ", ".join(f"'{k}'" for k in c["chokepoints"])
        con.execute(f"""
            INSERT INTO gold.crisis_chokepoints
            WITH now AS (SELECT portid, any_value(name) AS name, avg(transits) AS v FROM silver.transit_weeks
                         WHERE week {window} AND portid IN ({ids}) GROUP BY portid),
                 was AS (SELECT portid, avg(transits) AS v FROM silver.transit_weeks
                         WHERE week {prior} AND portid IN ({ids}) GROUP BY portid)
            SELECT '{c["id"]}', n.portid, n.name, round(n.v, 1), round(w.v, 1), round(100 * (n.v / w.v - 1), 1)
            FROM now n JOIN was w USING (portid) WHERE w.v > 0
        """)
        con.execute(f"""
            INSERT INTO gold.crisis_series
            WITH base AS (SELECT portid, avg(transits) AS b FROM silver.transit_weeks
                          WHERE portid IN ({ids}) AND week < DATE '{c['start']}' AND week >= DATE '{c['start']}' - {7 * INDEX_WEEKS}
                          GROUP BY portid)
            SELECT '{c["id"]}', t.name, t.week, round(100 * t.transits / b.b, 1)
            FROM silver.transit_weeks t JOIN base b USING (portid) WHERE b.b > 0
        """)


def build_evidence(con: duckdb.DuckDBPyConnection) -> None:
    """An alarm says traffic changed, not why. Grade how well each episode is backed up."""
    con.execute(f"""
        CREATE OR REPLACE TABLE gold.episode_evidence AS
        WITH e AS (SELECT e.*, p.lat, p.lon FROM gold.episodes e JOIN silver.ports p USING (portid)),
        logged AS (
            SELECT DISTINCT e.portid, e.episode FROM e
            JOIN silver.event_ports ep USING (portid) JOIN silver.events v USING (eventid)
            WHERE v.start_date <= e.end_week + 7
              AND coalesce(v.end_date, current_date) >= e.start_week - {7 * HIT_WEEKS_AFTER}
        ), near AS (
            SELECT a.portid, a.episode, count(DISTINCT b.portid) AS n FROM e a JOIN e b
              ON b.portid <> a.portid AND abs(b.start_week - a.start_week) <= 14
             AND 6371 * 2 * asin(sqrt(power(sin(radians(b.lat - a.lat) / 2), 2) + cos(radians(a.lat)) * cos(radians(b.lat))
                 * power(sin(radians(b.lon - a.lon) / 2), 2))) <= {NEIGHBOUR_KM}
            GROUP BY ALL
        )
        SELECT e.portid, e.episode,
               CASE WHEN l.portid IS NOT NULL THEN 'In the IMF log'
                    WHEN coalesce(n.n, 0) >= 2 THEN 'Nearby ports fell too'
                    ELSE 'This port only' END AS evidence
        FROM e LEFT JOIN logged l USING (portid, episode) LEFT JOIN near n USING (portid, episode)
    """)


def build_exposure(con: duckdb.DuckDBPyConnection) -> None:
    """Which countries lean on one port, and how much of their shipping sits in each crisis zone?"""
    con.execute("""
        CREATE OR REPLACE TABLE gold.country_exposure AS
        WITH recent AS (
            SELECT portid, avg(calls) AS calls FROM silver.port_weeks
            WHERE week > (SELECT max(week) - 364 FROM silver.port_weeks) GROUP BY portid
        ), c AS (
            SELECT p.iso3, any_value(p.country) AS country, p.portid, p.portname, r.calls, p.import_share, p.export_share
            FROM silver.ports p JOIN recent r USING (portid) WHERE p.iso3 IS NOT NULL GROUP BY p.iso3, p.portid, p.portname, r.calls, p.import_share, p.export_share
        ), t AS (
            SELECT *, sum(calls) OVER (PARTITION BY iso3) AS total,
                   row_number() OVER (PARTITION BY iso3 ORDER BY import_share DESC NULLS LAST, calls DESC) AS rk
            FROM c
        )
        SELECT iso3, any_value(country) AS country, count(*) AS ports, round(sum(calls)) AS weekly_calls,
               max(CASE WHEN rk = 1 THEN portname END) AS top_port,
               round(max(CASE WHEN rk = 1 THEN import_share END), 1) AS top_port_import_share,
               round(sum(power(100 * calls / nullif(total, 0), 2))) AS hhi
        FROM t GROUP BY iso3
    """)


def build_pressure_link(con: duckdb.DuckDBPyConnection) -> None:
    """Does the vessel stress index move with, or ahead of, the NY Fed's supply chain pressure index?"""
    con.execute("""
        CREATE OR REPLACE TABLE gold.monthly_pressure AS
        SELECT g.month, g.gscpi, s.stress
        FROM silver.gscpi g
        JOIN (SELECT date_trunc('month', week)::DATE AS month, avg(stress_index) AS stress
              FROM gold.global_weekly GROUP BY 1) s USING (month)
        ORDER BY month
    """)
    con.execute("""
        CREATE OR REPLACE TABLE gold.pressure_lags AS
        SELECT lag_months, round(corr(stress, gscpi), 3) AS correlation, count(*) AS months
        FROM (SELECT l.lag_months, m.stress, f.gscpi
              FROM (SELECT unnest(range(-6, 7)) AS lag_months) l
              CROSS JOIN gold.monthly_pressure m
              JOIN gold.monthly_pressure f ON f.month = m.month + to_months(l.lag_months))
        GROUP BY lag_months ORDER BY lag_months
    """)


def build_analysis(con: duckdb.DuckDBPyConnection) -> None:
    build_backtest(con)
    build_recovery(con)
    build_crises(con)
    build_evidence(con)
    build_exposure(con)
    build_pressure_link(con)
