import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RAW_PATH = "/Volumes/workspace/bank/raw/transactions/"

# ---------- BRONZE: raw files as they landed (Auto Loader, incremental) ----------
@dlt.table(comment="Raw bank transactions, ingested incrementally with Auto Loader")
def bronze_transactions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(RAW_PATH)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )

# ---------- SILVER: typed, validated, deduplicated ----------
@dlt.table(comment="Clean transactions: one row per txn_id (latest load wins)")
@dlt.expect_or_drop("account_id_present", "account_id IS NOT NULL")
@dlt.expect_or_drop("amount_present", "amount IS NOT NULL")
@dlt.expect("txn_date_not_in_future", "txn_date <= current_date()")
def silver_transactions():
    latest_first = Window.partitionBy("txn_id").orderBy(F.col("load_ts").desc())
    return (
        spark.read.table("bronze_transactions")
        .select(
            "txn_id",
            "account_id",
            F.to_date("txn_date").alias("txn_date"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            F.to_timestamp("load_ts").alias("load_ts"),
        )
        .withColumn("rn", F.row_number().over(latest_first))
        .filter("rn = 1")
        .drop("rn")
    )

# ---------- GOLD: daily net and running balance per account ----------
@dlt.table(comment="Daily net amount and running balance per account")
def gold_account_daily_balance():
    daily = (
        spark.read.table("silver_transactions")
        .groupBy("account_id", "txn_date")
        .agg(F.sum("amount").alias("daily_net"))
    )
    running = (
        Window.partitionBy("account_id")
        .orderBy("txn_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return daily.withColumn("running_balance", F.sum("daily_net").over(running))

# ---------- GOLD: daily reconciliation, source vs clean ----------
@dlt.table(comment="Daily reconciliation: source (latest per txn) vs silver counts and amounts")
def gold_reconciliation():
    raw = (
        spark.read.table("bronze_transactions")
        .select(
            "txn_id",
            F.to_date("txn_date").alias("txn_date"),
            F.col("amount").cast("decimal(18,2)").alias("amount"),
            F.to_timestamp("load_ts").alias("load_ts"),
        )
    )
    latest = Window.partitionBy("txn_id").orderBy(F.col("load_ts").desc())
    source = (
        raw.withColumn("rn", F.row_number().over(latest)).filter("rn = 1")
        .groupBy("txn_date")
        .agg(F.count("*").alias("source_txns"), F.sum("amount").alias("source_amount"))
    )
    target = (
        spark.read.table("silver_transactions")
        .groupBy("txn_date")
        .agg(F.count("*").alias("silver_txns"), F.sum("amount").alias("silver_amount"))
    )
    return (
        source.join(target, "txn_date", "left")
        .withColumn("txn_diff", F.col("source_txns") - F.coalesce(F.col("silver_txns"), F.lit(0)))
        .withColumn("amount_diff", F.coalesce(F.col("source_amount"), F.lit(0)) - F.coalesce(F.col("silver_amount"), F.lit(0)))
        .withColumn("status", F.when((F.col("txn_diff") == 0) & (F.col("amount_diff") == 0), "MATCHED").otherwise("CHECK"))
    )