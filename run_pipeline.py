"""Orchestrator: run steps 01–10 in sequence. Uses {DATA_ROOT}/{env}/{dataset}/pipeline.yaml (--env dev|prod, --dataset NAME) or --pipeline PATH."""
import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

import yaml

from lib.config import get_combined_path
from lib.data_root import get_data_root, get_dataset_path

ROOT = Path(__file__).resolve().parent
SCRIPTS_DIR = ROOT / "scripts"

DEFAULT_DATASET = "tasks"
ALL_DATASETS = ["tasks", "dept_mapping", "employees_master", "workers"]

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

# Steps 1-9 run per-dataset; step 10 (views) runs once after all datasets in --all mode
STEPS_PER_DATASET = [(n, name, path) for n, name, path in STEPS if n <= 9]
STEP_VIEWS = (10, "10_sqlite_views", SCRIPTS_DIR / "10_sqlite_views.py")


def _print_missing_pipeline_help(missing_path: Path) -> None:
    print(f"Pipeline config not found: {missing_path}", file=sys.stderr)
    print("", file=sys.stderr)
    print("Create the external data layout and copy config templates:", file=sys.stderr)
    print("  python scripts/init_data_directory.py", file=sys.stderr)
    print("If your data is still under the repo datasets/ folder:", file=sys.stderr)
    print("  python scripts/migrate_data.py --dry-run", file=sys.stderr)


def setup_logging(logs_dir: Path) -> logging.Logger:
    """Configure pipeline orchestrator logging to logs_dir/pipeline.log."""
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / "pipeline.log"
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


def get_analytics_output_path(dataset_root: Path) -> Path:
    """Path to combined Parquet under dataset analytics/ (from dataset config/combine.yaml)."""
    return get_combined_path(dataset_root / "analytics", dataset_root / "config" / "combine.yaml")


def remove_analytics_output(dataset_root: Path, log: logging.Logger) -> None:
    """Remove combined Parquet from external analytics/ so no partial output remains on failure."""
    path = get_analytics_output_path(dataset_root)
    if path.exists():
        try:
            path.unlink()
            log.warning("Removed partial output: %s", path)
        except OSError as e:
            log.error("Failed to remove partial output %s: %s", path, e)


def dry_run_validate(dataset_root: Path, log: logging.Logger) -> bool:
    """Validate configs and inputs under dataset_root. Return True if all checks pass."""
    config_dir = dataset_root / "config"
    raw_dir = dataset_root / "raw"
    clean_dir = dataset_root / "clean"
    ok = True
    for name in ("schema.yaml", "combine.yaml"):
        path = config_dir / name
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
    if (config_dir / "value_maps.yaml").exists():
        try:
            with open(config_dir / "value_maps.yaml", encoding="utf-8") as f:
                yaml.safe_load(f)
        except Exception as e:
            log.error("Config invalid value_maps.yaml: %s", e)
            ok = False
    if not raw_dir.is_dir():
        log.warning("raw/ not found (step 01 may need it)")
    if not clean_dir.is_dir() and not raw_dir.is_dir():
        log.warning("Neither raw/ nor clean/ present")
    return ok


def run_step(
    step_num: int,
    name: str,
    script_path: Path,
    dataset_root: Path,
    pipeline_env: str,
    log: logging.Logger,
    pipeline_from_step: int = 1,
    pipeline_force: bool = False,
) -> bool:
    """Run one step via subprocess with PIPELINE_DATASET_ROOT and PIPELINE_ENV set. Return True if exit code 0."""
    if not script_path.exists():
        log.error("Script not found: %s", script_path)
        return False
    log.info("Running step %d: %s", step_num, name)
    env = os.environ.copy()
    env["PIPELINE_DATASET_ROOT"] = str(dataset_root)
    env["PIPELINE_ENV"] = pipeline_env
    env["DATA_ROOT"] = str(get_data_root())
    env["PIPELINE_FROM_STEP"] = str(pipeline_from_step)
    if pipeline_force:
        env["PIPELINE_FORCE"] = "1"
    else:
        env.pop("PIPELINE_FORCE", None)
    try:
        # Stream output in real-time instead of capturing
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(ROOT),
            env=env,
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
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pipeline steps 01–10 in sequence.")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=None,
        metavar="PATH",
        help="External data root (sets DATA_ROOT). Default: DATA_ROOT env or sibling ExcelIngestion_Data.",
    )
    parser.add_argument(
        "--env",
        choices=("dev", "prod"),
        default="dev",
        metavar="ENV",
        help="Environment segment under DATA_ROOT (default: dev). Uses DATA_ROOT/ENV/NAME/pipeline.yaml with --dataset.",
    )
    parser.add_argument(
        "--pipeline",
        default=None,
        metavar="PATH",
        help="Path to pipeline.yaml (overrides --env and --dataset).",
    )
    parser.add_argument(
        "--dataset",
        default=None,
        metavar="NAME",
        help="Dataset name (e.g. tasks, dept_mapping). Uses DATA_ROOT/--env/NAME/pipeline.yaml.",
    )
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
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Use DEBUG log level so step timing and memory (monitor_step) are visible.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all datasets (tasks, dept_mapping, employees_master, workers) in sequence.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess all raw Excel files (ignore fingerprints); set PIPELINE_FORCE=1 for each step.",
    )
    args = parser.parse_args()

    if args.data_root is not None:
        os.environ["DATA_ROOT"] = str(Path(args.data_root).expanduser().resolve())

    # Handle --all flag: run all datasets in sequence
    if args.all:
        if args.pipeline or args.dataset:
            print("Error: --all cannot be combined with --pipeline or --dataset", file=sys.stderr)
            return 1
        datasets_to_run = ALL_DATASETS
    else:
        datasets_to_run = None  # Single dataset mode

    # Single dataset mode
    if datasets_to_run is None:
        if args.pipeline:
            pipeline_path = Path(args.pipeline)
            if not pipeline_path.is_absolute():
                pipeline_path = ROOT / pipeline_path
        elif args.dataset:
            pipeline_path = get_dataset_path(args.env, args.dataset) / "pipeline.yaml"
        else:
            pipeline_path = get_dataset_path(args.env, DEFAULT_DATASET) / "pipeline.yaml"
        return run_single_dataset(pipeline_path, args)

    # Multi-dataset mode (--all)
    # Run steps 1-9 for each dataset, then step 10 once at the end
    print(f"Running all datasets: {', '.join(datasets_to_run)}")
    print(f"Environment: {args.env}")
    print("Note: Steps 1-9 run per-dataset; step 10 (views) runs once at the end.")
    failed = []
    for dataset_name in datasets_to_run:
        pipeline_path = get_dataset_path(args.env, dataset_name) / "pipeline.yaml"
        print(f"\n{'='*60}")
        print(f"Dataset: {dataset_name} (steps 1-9)")
        print(f"{'='*60}")
        result = run_single_dataset(pipeline_path, args, skip_step_10=True)
        if result != 0:
            failed.append(dataset_name)
            print(f"WARNING: {dataset_name} failed, continuing with next dataset...")

    # Run step 10 once after all datasets (use first dataset's root for logging)
    if not failed or len(failed) < len(datasets_to_run):
        print(f"\n{'='*60}")
        if args.dry_run:
            print("Step 10 (sqlite_views) would run once for all datasets")
            print(f"{'='*60}")
        else:
            print("Running step 10 (sqlite_views) once for all datasets...")
            print(f"{'='*60}")
            # Use first successful dataset for logging context
            first_dataset = next((d for d in datasets_to_run if d not in failed), datasets_to_run[0])
            first_pipeline = get_dataset_path(args.env, first_dataset) / "pipeline.yaml"
            fp = first_pipeline.resolve()
            if not fp.is_file():
                print(f"Warning: Could not run step 10 - pipeline not found: {fp}", file=sys.stderr)
            else:
                dataset_root = fp.parent
                logs_dir = dataset_root / "logs"
                log = setup_logging(logs_dir)
                step_num, name, script_path = STEP_VIEWS
                if not run_step(
                    step_num,
                    name,
                    script_path,
                    dataset_root,
                    args.env,
                    log,
                    args.from_step,
                    args.force,
                ):
                    log.error("Step 10 (sqlite_views) failed.")
                    failed.append("step_10_views")

    if failed:
        print(f"\nCompleted with errors. Failed: {', '.join(failed)}")
        return 1
    print(f"\nAll datasets completed successfully.")
    return 0


def run_single_dataset(pipeline_path: Path, args: argparse.Namespace, skip_step_10: bool = False) -> int:
    """Run pipeline for a single dataset. Returns exit code.

    Args:
        pipeline_path: Path to pipeline.yaml
        args: Parsed command line arguments
        skip_step_10: If True, skip step 10 (sqlite_views). Used by --all mode
                      to run step 10 once after all datasets complete.
    """
    pipeline_path = pipeline_path.resolve()
    if not pipeline_path.is_file():
        _print_missing_pipeline_help(pipeline_path)
        return 1

    dataset_root = pipeline_path.parent

    logs_dir = dataset_root / "logs"
    log = setup_logging(logs_dir)
    if args.verbose:
        log.setLevel(logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("Data root: %s", get_data_root())
    log.info("Environment: %s", args.env)
    log.info("Dataset root: %s", dataset_root)

    if args.dry_run:
        log.info("Dry run: validating config and inputs (no writes).")
        if not dry_run_validate(dataset_root, log):
            return 1
        max_step = 9 if skip_step_10 else 10
        log.info("Dry run: would execute steps %d-%d: %s",
                 args.from_step,
                 max_step,
                 ", ".join(name for n, name, _ in STEPS if n >= args.from_step and n <= max_step))
        return 0

    if not (1 <= args.from_step <= 10):
        log.error("--from-step must be between 1 and 10, got %d", args.from_step)
        return 1

    # Determine which steps to run
    max_step = 9 if skip_step_10 else 10
    steps_to_run = [(n, name, path) for n, name, path in STEPS if n >= args.from_step and n <= max_step]
    if not steps_to_run:
        return 0

    log.info("Starting pipeline from step %d%s", args.from_step, " (skipping step 10)" if skip_step_10 else "")
    for step_num, name, script_path in steps_to_run:
        if not run_step(
            step_num,
            name,
            script_path,
            dataset_root,
            args.env,
            log,
            args.from_step,
            args.force,
        ):
            log.error("Pipeline stopped: step %d (%s) failed.", step_num, name)
            remove_analytics_output(dataset_root, log)
            return 1
    log.info("Pipeline completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
