# 03_aggregate.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, round as spark_round

spark = (
    SparkSession.builder
    .appName("paysim-aggregate")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 读取 DWD 层 ───────────────────────────────────────────────
dwd = spark.table("paysim.dwd_transactions")

# ── ADS 1：每日欺诈汇总 ───────────────────────────────────────
fraud_summary = (
    dwd.groupBy("tx_type", "tx_day")
    .agg(
        count("*").alias("total_cnt"),
        spark_sum("is_fraud").alias("fraud_cnt"),
    )
    .withColumn(
        "fraud_rate",
        spark_round(col("fraud_cnt") / col("total_cnt"), 6)
    )
    .orderBy("tx_type", "tx_day")
)

spark.sql("DROP TABLE IF EXISTS paysim.ads_fraud_summary")
spark.sql("""
    CREATE TABLE paysim.ads_fraud_summary (
        tx_type     STRING,
        tx_day      INT,
        total_cnt   BIGINT,
        fraud_cnt   BIGINT,
        fraud_rate  DOUBLE
    )
    STORED AS ORC
    TBLPROPERTIES ('orc.compress'='SNAPPY')
""")

fraud_summary.write.mode("overwrite").insertInto("paysim.ads_fraud_summary")
print("[aggregate] ads_fraud_summary 写入完成")
spark.sql("SELECT * FROM paysim.ads_fraud_summary").show(15)

# ── ADS 2：高风险账户 ─────────────────────────────────────────
high_risk = (
    dwd.filter(
        (col("is_large_amount") == 1) &
        (col("is_balance_zero_out") == 1) &
        (col("tx_type").isin("TRANSFER", "CASH_OUT"))
    )
    .groupBy("account_orig", "tx_type")
    .agg(
        count("*").alias("risk_tx_cnt"),
        spark_sum("amount").alias("total_amount"),
        spark_sum("is_fraud").alias("confirmed_fraud_cnt"),
    )
    .orderBy(col("risk_tx_cnt").desc())
)

spark.sql("DROP TABLE IF EXISTS paysim.ads_high_risk_accounts")
spark.sql("""
    CREATE TABLE paysim.ads_high_risk_accounts (
        account_orig        STRING,
        tx_type             STRING,
        risk_tx_cnt         BIGINT,
        total_amount        DOUBLE,
        confirmed_fraud_cnt BIGINT
    )
    STORED AS ORC
    TBLPROPERTIES ('orc.compress'='SNAPPY')
""")

high_risk.write.mode("overwrite").insertInto("paysim.ads_high_risk_accounts")
print("[aggregate] ads_high_risk_accounts 写入完成")
spark.sql("SELECT * FROM paysim.ads_high_risk_accounts LIMIT 20").show(truncate=False)

spark.stop()
