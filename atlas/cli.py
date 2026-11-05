import argparse
import sys
import time

from . import analysis, config, export, ingest, quality, transform


def build(paths: config.Paths, log=print) -> bool:
    """Silver, gold and the quality gate. Returns False if a critical check failed."""
    con = transform.connect(paths)
    try:
        started = time.time()
        transform.build_silver(con, paths)
        log(f"silver built in {time.time() - started:.1f}s")
        transform.build_scores(con)
        analysis.build_analysis(con)
        log(f"gold built in {time.time() - started:.1f}s")
        ok, _ = quality.run_checks(con, paths, log)
        return ok
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="atlas", description="Supply Shock Atlas pipeline")
    parser.add_argument("command", choices=["run", "ingest", "build", "export"])
    parser.add_argument("--full", action="store_true", help="re-download all history instead of the latest weeks")
    args = parser.parse_args(argv)
    paths = config.default_paths()

    if args.command in ("run", "ingest"):
        ingest.ingest(paths, full=args.full)
    if args.command in ("run", "build"):
        if not build(paths):
            print("critical data-quality check failed; nothing exported", file=sys.stderr)
            return 1
    if args.command in ("run", "export"):
        print(f"exported {export.export(paths)}")
    return 0
