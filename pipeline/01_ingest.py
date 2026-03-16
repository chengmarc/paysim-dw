from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = (
    SparkSession.builder
    .appName("PaySim-01-Ingest")
    .enableHiveSupport()
    .config("spark.sql.warehouse.dir",
            "hdfs://localhost:9000/user/hive/warehouse")
    .config("spark.hadoop.hive.exec.dynamic.partition", "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

RAW_CSV = "hdfs://localhost:9000/data/paysim/raw/raw_data.csv"

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RAW_CSV)
)

print(f"[ingest] 原始行数: {df_raw.count():,}")

# ODS 保留原始列名，仅添加分区列 tx_type
df = df_raw.withColumn("tx_type", trim(col("type")))
df.createOrReplaceTempView("stg_raw")

spark.sql("""
    INSERT OVERWRITE TABLE paysim.raw_transactions
    PARTITION (tx_type)
    SELECT
        step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
        nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud,
        tx_type
    FROM stg_raw
""")

print("[ingest] 分区行数验证:")
spark.sql("""
    SELECT tx_type, COUNT(*) AS row_cnt, SUM(isFraud) AS fraud_cnt
    FROM paysim.raw_transactions
    GROUP BY tx_type
    ORDER BY tx_type
""").show()

spark.stop()
print("[ingest] 完成")
