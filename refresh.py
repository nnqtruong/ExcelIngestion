#!/usr/bin/env python
"""
Full pipeline refresh: Python pipeline + dbt + verification.
Single command to go from raw Excel to Power BI-ready marts.

Usage:
    python refresh.py                    # full refresh (dev)
    python refresh.py --env prod         # production
    python refresh.py --skip-pipeline    # dbt only
    python refresh.py --skip-dbt         # pipeline only
    python refresh.py --dataset tasks    # single dataset + dbt
    python refresh.py --force            # force reprocess all files

dbt is always invoked from ``.venv-dbt`` only (``Scripts/dbt.exe`` or ``bin/dbt``),
i.e. the dbt CLI installed in the Python 3.12 dbt venv - not system ``dbt`` and not
the interpreter running ``refresh.py`` (which may be 3.14 for the pipeline).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_DBT = ROOT / ".venv-dbt"
VENV_DBT_DBT_WIN = VENV_DBT / "Scripts" / "dbt.exe"
VENV_DBT_DBT_NIX = VENV_DBT / "bin" / "dbt"
DBT_PROJECT = ROOT / "dbt_crc"


def _resolve_venv_dbt_cli() -> Path | None:
    """dbt entrypoint installed inside ``.venv-dbt`` (uses that venv's Python, e.g. 3.12)."""
    if VENV_DBT_DBT_WIN.exists():
        return VENV_DBT_DBT_WIN
    if VENV_DBT_DBT_NIX.exists():
        return VENV_DBT_DBT_NIX
    return None


def run_step(
    label: str,
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
) -> float:
    """Run a subprocess command with timing; exit on non-zero."""
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}\n")

    start = time.time()
    result = subprocess.run(cmd, cwd=str(cwd) if cwd else None, env=env)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"\n[FAILED] {label} (exit code {result.returncode})")
        sys.exit(result.returncode)

    print(f"\n[OK] {label} completed in {elapsed:.1f}s")
    return elapsed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Full pipeline + dbt refresh",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "prod"],
        help="Environment (default: dev)",
    )
    parser.add_argument(
        "--dataset",
        help="Single dataset to process (default: all)",
    )
    parser.add_argument(
        "--skip-pipeline",
        action="store_true",
        help="Skip Python pipeline, run dbt only",
    )
    parser.add_argument(
        "--skip-dbt",
        action="store_true",
        help="Skip dbt, run pipeline only",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reprocess all files (ignore fingerprint cache)",
    )
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  REFRESH - Environment: {args.env}")
    print(f"{'='*60}")

    total_start = time.time()
    timings: dict[str, float] = {}

    if not args.skip_pipeline:
        cmd = [sys.executable, str(ROOT / "run_pipeline.py"), "--env", args.env]
        if args.dataset:
            cmd += ["--dataset", args.dataset]
        else:
            cmd += ["--all"]
        if args.force:
            cmd += ["--force"]
        timings["pipeline"] = run_step("Python Pipeline", cmd, cwd=ROOT)

    if not args.skip_dbt:
        dbt_cli = _resolve_venv_dbt_cli()
        if dbt_cli is None:
            print(f"\n[WARNING] dbt CLI not found in .venv-dbt (expected {VENV_DBT_DBT_WIN} or {VENV_DBT_DBT_NIX})")
            print("Skipping dbt. Create the venv with Python 3.12 and install dbt, e.g.:")
            print("  py -3.12 -m venv .venv-dbt")
            print("  .venv-dbt\\Scripts\\pip install dbt-core dbt-duckdb")
        else:
            from lib.data_root import get_data_root

            dbt_env = os.environ.copy()
            dbt_env.setdefault("DATA_ROOT", str(get_data_root()))
            dbt_env["PIPELINE_ENV"] = args.env

            timings["dbt_run"] = run_step(
                "dbt run",
                [str(dbt_cli), "run"],
                cwd=DBT_PROJECT,
                env=dbt_env,
            )
            timings["dbt_test"] = run_step(
                "dbt test",
                [str(dbt_cli), "test"],
                cwd=DBT_PROJECT,
                env=dbt_env,
            )

    total = time.time() - total_start
    print(f"\n{'='*60}")
    print("  REFRESH COMPLETE")
    print(f"{'='*60}")
    print(f"  Total time: {total:.1f}s")
    for step, t in timings.items():
        print(f"    {step}: {t:.1f}s")
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
