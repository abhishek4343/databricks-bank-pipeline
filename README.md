# Bank Transactions Lakehouse Pipeline (Databricks Lakeflow Declarative Pipelines)

A medallion (bronze → silver → gold) pipeline on Databricks that ingests daily bank transaction files incrementally, removes duplicates from pipeline re-runs, enforces data quality rules, applies late corrections, and reconciles the clean data back to source.

Built on Databricks Free Edition (serverless) with Lakeflow Declarative Pipelines (formerly Delta Live Tables), Auto Loader, Delta Lake and Unity Catalog volumes.

## Architecture

```
CSV files in a Unity Catalog volume
        │  Auto Loader (incremental, only new files)
        ▼
bronze_transactions        raw rows as landed + ingestion metadata (_ingested_at, _source_file)
        │  typing, ROW_NUMBER dedupe (latest load_ts wins), expectations
        ▼
silver_transactions        one clean row per txn_id
        │
        ├──► gold_account_daily_balance   daily net amount and running balance per account
        └──► gold_reconciliation          per business date: source vs silver counts and amounts, MATCHED / CHECK
```

## What it demonstrates

| Problem | How the pipeline handles it |
| --- | --- |
| Pipeline re-runs load the same transaction twice | `ROW_NUMBER()` over `txn_id` ordered by `load_ts DESC`, keep row 1 |
| Source re-sends a record with a corrected amount | The later `load_ts` wins, so the correction replaces the old value |
| Bad records (missing account or amount) | `@dlt.expect_or_drop` rules drop them and report counts on every run |
| Soft rules (e.g. no future dates) | `@dlt.expect` warns without dropping |
| Only new files should be processed | Auto Loader (`cloudFiles`) tracks which files were already ingested |
| Proving the clean data matches source | `gold_reconciliation` compares counts and amounts per date and flags `CHECK` |

## Results from the test data

- Batch 1: 1,052 raw rows (1,000 transactions, 50 re-run duplicates, 2 bad rows)
- Silver: 1,000 clean rows; data quality report shows 2 dropped (1 missing account, 1 missing amount)
- Batch 2: 3 rows (2 new transactions + 1 corrected amount for `T00001`); Auto Loader processed only the new file and `T00001` updated from -4674.86 to -4600.00
- Reconciliation: 30 of 31 dates `MATCHED`; 2026-10-15 flagged `CHECK` with `txn_diff = 2` and `amount_diff = 100`, exactly the two rows dropped by the quality rules

## Files

- `00_generate_data.ipynb`: creates the schema and volume, and writes synthetic transaction batches (with duplicates, bad rows and a correction)
- `my_transformation.py`: the declarative pipeline (bronze, silver, gold daily balance, gold reconciliation)

## How to run

1. In Databricks (Free Edition works), run `00_generate_data.ipynb` to create `workspace.bank.raw` and write `batch_001.csv`.
2. Create an ETL pipeline (Jobs & Pipelines → Create → ETL pipeline), catalog `workspace`, and paste `my_transformation.py` as the transformation file.
3. Run the pipeline. Then run the batch 2 cell and run the pipeline again to see incremental processing and the correction.
4. Query `gold_reconciliation` and `gold_account_daily_balance`.

## Next steps

- Replace the window-based dedupe with `AUTO CDC` (apply changes) for change-data-capture style upserts and SCD Type 2 history
- Schedule the pipeline as a job and add alerting on `CHECK` rows
