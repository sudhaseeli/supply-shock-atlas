"""Bronze: page daily vessel records out of the IMF PortWatch API into Parquet.

The API is flaky: services flip to 'Invalid URL' or 504 for minutes at a time, including while
the IMF republishes them. So every request retries with backoff, each port is written atomically
and checked against a count from the server, and an interrupted run resumes where it stopped.
"""
import gzip
import json
import random
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import duckdb

from . import config

CALL_COLUMNS = ("{date:'DATE',portid:'VARCHAR',portcalls_container:'INT',portcalls_dry_bulk:'INT',"
                "portcalls_general_cargo:'INT',portcalls_roro:'INT',portcalls_tanker:'INT',portcalls:'INT',"
                "\"import\":'DOUBLE',\"export\":'DOUBLE'}")


def fetch(url: str, params: dict | None = None, retries: int = 6) -> bytes:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "supply-shock-atlas/0.1", "Accept-Encoding": "gzip"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=90) as r:
                body = r.read()
                return gzip.decompress(body) if r.headers.get("Content-Encoding") == "gzip" else body
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt + random.random())
    raise AssertionError("unreachable")


def query(url: str, retries: int = 80, **params) -> dict:
    for attempt in range(retries):
        body = json.loads(fetch(url, {"f": "json", "returnGeometry": "false", **params}))
        if "error" not in body:
            return body
        if attempt == retries - 1:
            raise RuntimeError(f"{url}: {body['error']}")
        # outages last minutes, so keep polling at a steady pace for up to ~40 minutes
        time.sleep(min(30, 3 * 2 ** attempt) + random.random() * 3)
    raise AssertionError("unreachable")


def fetch_all(url: str, **params) -> list[dict]:
    rows, offset = [], 0
    while True:
        page = query(url, resultOffset=offset, **params)
        rows += [f["attributes"] for f in page["features"]]
        if not page.get("exceededTransferLimit"):
            return rows
        offset += len(page["features"])


def write_parquet(rows: list[dict], shard, columns: str | None = None) -> None:
    """Atomic: the parquet only appears once it is complete."""
    tmp, part = shard.with_suffix(".json"), shard.with_suffix(".part")
    tmp.write_text(json.dumps(rows))
    cols = f", columns={columns}" if columns else ""
    con = duckdb.connect()
    con.execute(f"COPY (SELECT * FROM read_json('{tmp.as_posix()}', maximum_object_size=1000000000{cols})) "
                f"TO '{part.as_posix()}' (FORMAT parquet)")
    con.close()
    part.replace(shard)
    tmp.unlink()


def load_port(paths: config.Paths, run: str, portid: str, where: str) -> int:
    shard = paths.calls / f"run={run}" / f"{portid}.parquet"
    if shard.exists():
        return 0
    scope = f"portid='{portid}' AND {where}"
    expected = query(config.DAILY_PORTS, where=scope, returnCountOnly="true")["count"]
    if expected == 0:
        return 0
    rows = []
    for _ in range(3):
        rows = fetch_all(config.DAILY_PORTS, where=scope, outFields=",".join(config.DAILY_FIELDS),
                         orderByFields="date", resultRecordCount=config.PAGE_SIZE)
        if len(rows) == expected:
            break
    else:
        raise RuntimeError(f"{portid}: got {len(rows)} rows, server says {expected}")
    write_parquet(rows, shard, CALL_COLUMNS)
    return len(rows)


def ingest_reference(paths: config.Paths, log=print) -> None:
    paths.bronze.mkdir(parents=True, exist_ok=True)
    ports = fetch_all(config.PORTS, where="1=1", outFields="*", orderByFields="ObjectId")
    (paths.bronze / "ports.json").write_text(json.dumps(ports))
    events = fetch_all(config.DISRUPTIONS, where="1=1", outFields="*", orderByFields="ObjectId")
    (paths.bronze / "disruptions.json").write_text(json.dumps(events))
    chokes = fetch_all(config.CHOKEPOINTS, where="1=1", outFields="*", orderByFields="ObjectId")
    (paths.bronze / "chokepoints.json").write_text(json.dumps(chokes))
    (paths.bronze / "gscpi.xls").write_bytes(fetch(config.GSCPI_URL))
    log(f"reference: {len(ports)} ports, {len(events)} disruptions, {len(chokes)} chokepoints, NY Fed pressure index")


def ingest_transits(paths: config.Paths, log=print) -> None:
    """Daily chokepoint transits are small enough to replace on every run."""
    shard = paths.bronze / "chokepoint_transits.parquet"
    expected = query(config.DAILY_CHOKEPOINTS, where="1=1", returnCountOnly="true")["count"]
    rows = fetch_all(config.DAILY_CHOKEPOINTS, where="1=1", outFields="*", orderByFields="ObjectId")
    if len(rows) != expected:
        raise RuntimeError(f"chokepoint transits: got {len(rows)} rows, server says {expected}")
    write_parquet(rows, shard)
    log(f"chokepoint transits: {len(rows):,} rows")


def watermark(paths: config.Paths) -> date | None:
    runs = paths.bronze / "runs.json"
    if not runs.exists():
        return None
    done = [r for r in json.loads(runs.read_text()) if r.get("complete")]
    return date.fromisoformat(max(r["max_date"] for r in done)) if done else None


def ingest_calls(paths: config.Paths, full: bool = False, log=print) -> None:
    wm = None if full else watermark(paths)
    since = wm - timedelta(days=config.REVISION_DAYS) if wm else None
    # only days with traffic are requested; silver rebuilds the zero days
    where = "portcalls>0" + (f" AND date>='{since}'" if since else "")
    # a run id derived from the window means an interrupted run resumes its existing shards
    run = f"from_{since}" if since else "full"
    ports = json.loads((paths.bronze / "ports.json").read_text())
    ports = [p["portid"] for p in sorted(ports, key=lambda p: -(p["vessel_count_total"] or 0))]
    stat = [{"statisticType": "max", "onStatisticField": "date", "outStatisticFieldName": "d"}]
    max_date = query(config.DAILY_PORTS, where="1=1", outStatistics=json.dumps(stat))["features"][0]["attributes"]["d"]
    log(f"run {run}: {len(ports)} ports, data to {max_date}")
    (paths.calls / f"run={run}").mkdir(parents=True, exist_ok=True)

    started, done, loaded, failed = time.time(), 0, 0, []
    with ThreadPoolExecutor(config.WORKERS) as pool:
        jobs = {pool.submit(load_port, paths, run, p, where): p for p in ports}
        for job in as_completed(jobs):
            try:
                loaded += job.result()
            except Exception as e:
                failed.append(jobs[job])
                log(f"  {jobs[job]} failed, re-run to resume: {e}")
            done += 1
            if done % 50 == 0 or done == len(jobs):
                log(f"  {done}/{len(jobs)} ports, {loaded:,} rows, {time.time() - started:.0f}s")
    if failed:
        raise RuntimeError(f"{len(failed)} ports failed; run `atlas ingest` again to resume")

    runs_file = paths.bronze / "runs.json"
    runs = json.loads(runs_file.read_text()) if runs_file.exists() else []
    runs = [r for r in runs if r["run"] != run] + [{
        "run": run, "complete": True, "since": str(since) if since else None, "max_date": max_date,
        "ports": len(ports), "loaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }]
    runs_file.write_text(json.dumps(runs, indent=2))


def ingest(paths: config.Paths, full: bool = False, log=print) -> None:
    ingest_reference(paths, log)
    ingest_calls(paths, full, log)
    try:
        ingest_transits(paths, log)
    except Exception as e:
        # the chokepoint feed can stay down for many hours; the atlas still works from port calls
        log(f"chokepoint feed unavailable, keeping any earlier copy: {e}")
