"""A tiny synthetic world with the behaviours the pipeline must get right, so tests run offline."""
import json
from datetime import date, timedelta

import duckdb
import pytest
import xlwt

from atlas import config

START = date(2022, 1, 3)
WEEKS = 170
SHOCK_WEEK = 150          # HUB goes dark here, for 8 weeks
QUIET_SHOCK_END = SHOCK_WEEK + 8


def daily_calls(port: str, day: date, week: int) -> int:
    if port == "HUB":
        if SHOCK_WEEK <= week < QUIET_SHOCK_END:
            return 0
        return 10 + (day.toordinal() % 3)
    if port == "SEASONAL":
        # shuts every year in the week containing 1 January
        return 0 if (day + timedelta(days=6 - day.weekday())).year > day.year else 8 + (day.toordinal() % 2)
    if port == "STEADY":
        return 6 + (day.toordinal() % 2)
    return 0  # TINY never has traffic


@pytest.fixture
def paths(tmp_path):
    p = config.Paths(tmp_path / "data")
    p.calls.joinpath("run=full").mkdir(parents=True)
    ports = [
        {"portid": name, "portname": name.title(), "country": "Testland", "ISO3": "TST", "continent": "Asia",
         "lat": 1.0, "lon": 2.0, "vessel_count_total": 100, "vessel_count_container": 10, "vessel_count_tanker": 10,
         "industry_top1": "Test", "share_country_maritime_import": share, "share_country_maritime_export": share}
        for name, share in [("HUB", 60.0), ("SEASONAL", 30.0), ("STEADY", 10.0), ("TINY", None)]
    ]
    (p.bronze / "ports.json").write_text(json.dumps(ports))
    (p.bronze / "chokepoints.json").write_text(json.dumps([
        {"portid": "chokepoint1", "portname": "Suez Canal", "lat": 30.0, "lon": 32.0, "vessel_count_total": 100,
         "vessel_count_container": 10, "vessel_count_tanker": 10, "industry_top1": "Test"}]))
    start_ms = int(((START + timedelta(weeks=SHOCK_WEEK)) - date(1970, 1, 1)).total_seconds()) * 1000
    event = {"eventid": 1, "eventtype": "OT", "eventname": "TEST BLOCKADE", "htmlname": "Test", "htmldescription": "Test",
             "alertlevel": "RED", "country": "Testland", "fromdate": start_ms, "todate": start_ms + 7 * 86400000,
             "lat": 1.0, "long": 2.0, "affectedports": "HUB", "n_affectedports": 1, "editdate": 1}
    (p.bronze / "disruptions.json").write_text(json.dumps([event]))

    con = duckdb.connect()
    con.execute("""CREATE TABLE c (date DATE, portid VARCHAR, portcalls_container INT, portcalls_dry_bulk INT,
                   portcalls_general_cargo INT, portcalls_roro INT, portcalls_tanker INT, portcalls INT,
                   "import" DOUBLE, "export" DOUBLE)""")
    rows = []
    for i in range(WEEKS * 7):
        day = START + timedelta(days=i)
        for port in ("HUB", "SEASONAL", "STEADY"):
            n = daily_calls(port, day, i // 7)
            if n:
                rows.append((day, port, n - 1, 0, 0, 0, 1, n, n * 100.0, n * 50.0))
    con.executemany("INSERT INTO c VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    for port in ("HUB", "SEASONAL", "STEADY"):
        con.execute(f"COPY (SELECT * FROM c WHERE portid = '{port}') TO '{(p.calls / 'run=full' / f'{port}.parquet').as_posix()}' (FORMAT parquet)")
    con.execute(f"""COPY (SELECT DATE '2022-01-03' + INTERVAL (i) DAY AS date, 'chokepoint1' AS portid, 'Suez Canal' AS portname,
                                 50 AS n_total, 20 AS n_container, 15 AS n_tanker, 10 AS n_dry_bulk, 1000.0 AS capacity
                          FROM range({WEEKS * 7}) t(i)) TO '{(p.bronze / 'chokepoint_transits.parquet').as_posix()}' (FORMAT parquet)""")
    con.close()

    book = xlwt.Workbook()
    sheet = book.add_sheet("GSCPI Monthly Data")
    sheet.write(0, 0, "Date")
    sheet.write(0, 1, "GSCPI")
    for i, (y, m) in enumerate((y, m) for y in range(2022, 2026) for m in range(1, 13)):
        sheet.write(i + 1, 0, date(y, m, 28).strftime("%d-%b-%Y"))
        sheet.write(i + 1, 1, float(m) / 10)
    book.save(str(p.bronze / "gscpi.xls"))
    return p
