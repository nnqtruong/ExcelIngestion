"""
Run pipeline at medium or large scale and verify timing/memory limits.
Parses pipeline.log for monitor_step lines and reports per-step stats.

Usage:
  python tests/run_stress_test.py --scale medium   # < 2 min total, < 1GB per step
  python tests/run_stress_test.py --scale large    # < 10 min total, < 1.5GB per step
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATASET_ROOT = ROOT / "datasets" / "tasks"
LOG_FILE = DATASET_ROOT / "logs" / "pipeline.log"

# monitor_step log line: run_convert | 0.2s | RAM: 94MB -> 99MB (delta: 5MB)
MONITOR_PATTERN = re.compile(
    r"(\w+)\s+\|\s+([\d.]+)s\s+\|\s+RAM:\s+[\d.]+MB\s+(?:->|→)\s+([\d.]+)MB"
)


# Expected number of pipeline steps that emit monitor_step
EXPECTED_STEPS = 10


def parse_log(log_path: Path) -> list[dict]:
    """Extract step name, elapsed seconds, and RAM after (MB) from pipeline.log.
    Returns the most recent run only (last EXPECTED_STEPS matches)."""
    if not log_path.exists():
        return []
    results = []
    with open(log_path, encoding="utf-8") as f:
        for line in f:
            m = MONITOR_PATTERN.search(line)
            if m:
                results.append({
                    "step": m.group(1),
                    "elapsed_s": float(m.group(2)),
                    "ram_mb": float(m.group(3)),
                })
    return results[-EXPECTED_STEPS:] if len(results) >= EXPECTED_STEPS else results


def run_stress_test(scale: str, skip_fixtures: bool = False) -> tuple[bool, list[dict], float]:
    """
    Optionally create fixtures, then run pipeline for given scale.
    Returns (success, list of step stats, total_elapsed from start to end).
    """
    import time
    t0 = time.time()

    if not skip_fixtures:
        # 1. Create fixtures
        create_cmd = [sys.executable, str(ROOT / "tests" / "create_fixtures.py"), "--scale", scale]
        cp = subprocess.run(create_cmd, cwd=str(ROOT), timeout=3600, capture_output=True, text=True)
        if cp.returncode != 0:
            print(cp.stderr or cp.stdout)
            return False, [], time.time() - t0

    # 2. Run pipeline (stream output)
    pipeline_cmd = [sys.executable, str(ROOT / "run_pipeline.py"), "--dataset", "tasks"]
    cp = subprocess.run(pipeline_cmd, cwd=str(ROOT), timeout=900, capture_output=False)
    if cp.returncode != 0:
        return False, parse_log(LOG_FILE), time.time() - t0

    total_elapsed = time.time() - t0
    return True, parse_log(LOG_FILE), total_elapsed


def main():
    parser = argparse.ArgumentParser(description="Stress test pipeline at medium or large scale.")
    parser.add_argument("--scale", choices=["medium", "large"], required=True)
    parser.add_argument(
        "--skip-fixtures",
        action="store_true",
        help="Skip fixture creation; run pipeline only (use when large fixtures already exist).",
    )
    args = parser.parse_args()

    scale = args.scale
    if scale == "medium":
        time_limit_s = 2 * 60  # 2 minutes
        ram_limit_mb = 1024     # 1 GB
    else:
        time_limit_s = 10 * 60  # 10 minutes
        ram_limit_mb = 1536     # 1.5 GB

    print(f"Stress test: scale={scale}")
    print(f"  Limits: pipeline time < {time_limit_s // 60} min, each step RAM < {ram_limit_mb} MB")
    if args.skip_fixtures:
        print("  (skipping fixture creation)")
    print()

    ok, steps, total_elapsed = run_stress_test(scale, skip_fixtures=args.skip_fixtures)
    if not ok:
        print("Pipeline or fixture creation failed.")
        return 1

    # Report
    print("\n--- Per-step timing and memory ---")
    print(f"{'Step':<28} {'Time (s)':>10} {'RAM (MB)':>10}")
    print("-" * 50)
    pipeline_time = 0.0
    max_ram = 0.0
    for s in steps:
        print(f"{s['step']:<28} {s['elapsed_s']:>10.1f} {s['ram_mb']:>10.0f}")
        pipeline_time += s["elapsed_s"]
        max_ram = max(max_ram, s["ram_mb"])
    print("-" * 50)
    print(f"{'Total (from log steps)':<28} {pipeline_time:>10.1f} {max_ram:>10.0f}")
    print(f"\nWall-clock total (fixtures + pipeline): {total_elapsed:.1f}s")

    # Verify limits: time limit applies to pipeline steps total (not fixture creation)
    time_ok = pipeline_time <= time_limit_s
    ram_ok = max_ram <= ram_limit_mb
    if time_ok and ram_ok:
        print(f"\nPASS: pipeline time {pipeline_time:.1f}s <= {time_limit_s}s, max RAM {max_ram:.0f} MB <= {ram_limit_mb} MB")
        return 0
    if not time_ok:
        print(f"\nFAIL: pipeline time {pipeline_time:.1f}s exceeds limit {time_limit_s}s")
    if not ram_ok:
        print(f"\nFAIL: max step RAM {max_ram:.0f} MB exceeds limit {ram_limit_mb} MB")
    return 1


if __name__ == "__main__":
    sys.exit(main())
