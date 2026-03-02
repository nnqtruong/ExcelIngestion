"""Orchestrator: run steps 01–10 in sequence. Supports --dry-run and --from-step N."""

import argparse
import logging
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR = ROOT / "scripts"
CONFIG_DIR = ROOT / "config"
ANALYTICS_DIR = ROOT / "analytics"
LOGS_DIR = ROOT / "logs"

STEPS = [
    (1, "01_convert", SCRIPTS_DIR / "01_convert.py"),
    (2, "02_normalize_schema", SCRIPTS_DIR / "02_normalize_schema.py"),
    (3, "03_add_missing_columns", SCRIPTS_DIR / "03_add_missing_columns.py"),
    (4, "04_clean_errors", SCRIPTS_DIR / "04_clean_errors.py"),
    (5, "05_normalize_values", SCRIPTS_DIR / "05_normalize_values.py"),
    (6, "06_combine_datasets", SCRIPTS_DIR / "06_combine_datasets.py"),
    (7, "07_handle_nulls", SCRIPTS_DIR / "07_handle_nulls.py"),
    (8, "08_validate", SCRIPTS_DIR / "08_validate.py"),
    (9, "09_export_sqlite", SCRIPTS_DIR / "09_export_sqlite.py"),
    (10, "10_sqlite_views", SCRIPTS_DIR / "10_sqlite_views.py"),
]


def setup_logging() -> logging.Logger:
    """Configure pipeline orchestrator logging."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / "pipeline.log"
    logger = logging.getLogger("run_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def get_analytics_output_path() -> Path | None:
    """Path to combined output in analytics/ (from combine.yaml), or None."""
    combine_path = CONFIG_DIR / "combine.yaml"
    if not combine_path.exists():
        return None
    with open(combine_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if config and config.get("output"):
        return ANALYTICS_DIR / config["output"]
    return ANALYTICS_DIR / "combined.parquet"


def remove_analytics_output(log: logging.Logger) -> None:
    """Remove combined output from analytics/ so no partial output remains on failure."""
    path = get_analytics_output_path()
    if path and path.exists():
        try:
            path.unlink()
            log.warning("Removed partial output: %s", path)
        except OSError as e:
            log.error("Failed to remove partial output %s: %s", path, e)


def dry_run_validate(log: logging.Logger) -> bool:
    """Validate configs and inputs without writing. Return True if all checks pass."""
    ok = True
    for path in (CONFIG_DIR / "schema.yaml", CONFIG_DIR / "combine.yaml"):
        if not path.exists():
            log.error("Config missing: %s", path)
            ok = False
        else:
            try:
                with open(path, encoding="utf-8") as f:
                    yaml.safe_load(f)
            except Exception as e:
                log.error("Config invalid %s: %s", path, e)
                ok = False
    if (CONFIG_DIR / "value_maps.yaml").exists():
        try:
            with open(CONFIG_DIR / "value_maps.yaml", encoding="utf-8") as f:
                yaml.safe_load(f)
        except Exception as e:
            log.error("Config invalid value_maps.yaml: %s", e)
            ok = False
    raw_dir = ROOT / "raw"
    clean_dir = ROOT / "clean"
    if not raw_dir.is_dir():
        log.warning("raw/ not found (step 01 may need it)")
    if not clean_dir.is_dir() and not raw_dir.is_dir():
        log.warning("Neither raw/ nor clean/ present")
    return ok


def run_step(step_num: int, name: str, script_path: Path, log: logging.Logger) -> bool:
    """Run one step via subprocess. Return True if exit code 0."""
    if not script_path.exists():
        log.error("Script not found: %s", script_path)
        return False
    log.info("Running step %d: %s", step_num, name)
    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=3600,
        )
    except subprocess.TimeoutExpired:
        log.error("Step %d (%s) timed out", step_num, name)
        return False
    except Exception as e:
        log.error("Step %d (%s) failed: %s", step_num, name, e)
        return False
    if result.returncode != 0:
        log.error("Step %d (%s) failed with exit code %d", step_num, name, result.returncode)
        if result.stdout:
            log.error("stdout: %s", result.stdout.strip())
        if result.stderr:
            log.error("stderr: %s", result.stderr.strip())
        return False
    if result.stdout:
        log.debug("stdout: %s", result.stdout.strip())
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pipeline steps 01–10 in sequence.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configs and inputs only; do not run steps or write output.",
    )
    parser.add_argument(
        "--from-step",
        type=int,
        default=1,
        metavar="N",
        help="Start from step N (1–10). Default: 1.",
    )
    args = parser.parse_args()

    log = setup_logging()

    if args.dry_run:
        log.info("Dry run: validating config and inputs (no writes).")
        if not dry_run_validate(log):
            return 1
        log.info("Dry run: would execute steps %d-10: %s",
                 args.from_step,
                 ", ".join(name for n, name, _ in STEPS if n >= args.from_step))
        return 0

    if not (1 <= args.from_step <= 10):
        log.error("--from-step must be between 1 and 10, got %d", args.from_step)
        return 1

    steps_to_run = [(n, name, path) for n, name, path in STEPS if n >= args.from_step]
    if not steps_to_run:
        return 0

    log.info("Starting pipeline from step %d", args.from_step)
    for step_num, name, script_path in steps_to_run:
        if not run_step(step_num, name, script_path, log):
            log.error("Pipeline stopped: step %d (%s) failed.", step_num, name)
            remove_analytics_output(log)
            return 1
    log.info("Pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
