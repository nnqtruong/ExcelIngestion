# Stress test results

## Scale definitions

| Scale  | Rows per file | Total rows | Use case                    |
|--------|----------------|------------|-----------------------------|
| small  | 100            | 1,200      | Fast unit/integration tests |
| medium | 10,000         | 120,000    | CI / < 2 min, < 1 GB       |
| large  | 500,000        | 6,000,000  | Stress / < 10 min, < 1.5 GB |

## Medium scale (10K rows/file, 120K total)

**Limits:** Pipeline time < 2 min, each step RAM < 1 GB.

| Step                     | Time (s) | RAM (MB) |
|--------------------------|----------|----------|
| run_convert              | 14.1     | 127      |
| run_normalize_schema      | 0.4      | 43       |
| run_add_missing_columns   | 0.4      | 42       |
| run_clean_errors          | 0.7      | 43       |
| run_normalize_values      | 0.3      | 42       |
| run_combine_datasets      | 0.2      | 41       |
| run_handle_nulls          | 0.0      | 40       |
| run_validate              | 0.1      | 41       |
| run_export_sqlite         | 2.7      | 212      |
| run_sqlite_views          | 10.9     | 24       |
| **Total (pipeline)**      | **29.8** | **212**  |

- **Result:** PASS (pipeline 29.8s < 120s, max RAM 212 MB < 1024 MB).
- Wall-clock including fixture creation: ~59 s.

## Large scale (500K rows/file, 6M total)

**Limits:** Pipeline time < 10 min, each step RAM < 1.5 GB.

Generating 12 × 500K row Excel files can take **30+ minutes**. To stress-test the pipeline only:

1. Generate fixtures once:  
   `python tests/create_fixtures.py --scale large`
2. Run stress test without recreating fixtures:  
   `python tests/run_stress_test.py --scale large --skip-fixtures`

Then check the printed table: pipeline time should be < 600 s and max step RAM < 1536 MB.

*(Run large-scale test and paste per-step timing/memory here when available.)*
