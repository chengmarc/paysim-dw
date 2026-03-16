"""
02_transform.py
PaySim 数仓 — DWD 层 Transform
读取：paysim.raw_transactions
写入：paysim.dwd_transactions（ORC + Snappy，双分区 tx_type / tx_day）
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── 业务参数 ────────────────────────────────────────────────────────────────
LARGE_AMOUNT_THRESHOLD = 200_000.0   # 大额交易阈值（业务规则参数，可按需调整）
SOURCE_TABLE  = "paysim.raw_transactions"
TARGET_TABLE  = "paysim.dwd_transactions"

# ── SparkSession ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("paysim_transform")
    .enableHiveSupport()
    # 动态分区：必须在 build 阶段注入，运行时 SET 对已固化 Hive context 无效
    .config("spark.hadoop.hive.exec.dynamic.partition",        "true")
    .config("spark.hadoop.hive.exec.dynamic.partition.mode",   "nonstrict")
    .config("spark.sql.orc.compression.codec", "snappy")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── 读取 ODS 层 ──────────────────────────────────────────────────────────────
print("[transform] 读取原始表 ...")
raw = spark.table(SOURCE_TABLE)

# ── Transform ────────────────────────────────────────────────────────────────
print("[transform] 应用列重命名与特征工程 ...")

dwd = (
    raw
    # 1. 列重命名：ODS 原始列名 → DWD 业务列名
    #    tx_type 已在 01_ingest.py 写入时重命名，此处不再处理
    .withColumnRenamed("nameOrig",       "account_orig")
    .withColumnRenamed("oldbalanceOrg",  "balance_orig_before")
    .withColumnRenamed("newbalanceOrig", "balance_orig_after")
    .withColumnRenamed("nameDest",       "account_dest")
    .withColumnRenamed("oldbalanceDest", "balance_dest_before")
    .withColumnRenamed("newbalanceDest", "balance_dest_after")
    .withColumnRenamed("isFraud",        "is_fraud")
    .withColumnRenamed("isFlaggedFraud", "is_flagged_fraud")

    # 2. 派生：交易日（step 为小时，PaySim 模拟 30 天 = 744 小时）
    #    tx_day ∈ [0, 29]，用作第二层分区键
    .withColumn("tx_day", (F.col("step") / 24).cast("int"))

    # 3. 余额差异
    .withColumn("balance_diff_orig",
                F.col("balance_orig_before") - F.col("balance_orig_after"))
    .withColumn("balance_diff_dest",
                F.col("balance_dest_after") - F.col("balance_dest_before"))

    # 4. 大额标记
    .withColumn("is_large_amount",
                (F.col("amount") > LARGE_AMOUNT_THRESHOLD).cast("int"))

    # 5. 余额归零标记：发起方转账前有余额、转后归零
    .withColumn("is_balance_zero_out",
                (
                    (F.col("balance_orig_before") > 0) &
                    (F.col("balance_orig_after")  == 0)
                ).cast("int"))
)

# ── 列顺序：分区列（tx_type, tx_day）必须置于末尾 ────────────────────────────
dwd = dwd.select(
    "step",
    "amount",
    "account_orig",
    "balance_orig_before",
    "balance_orig_after",
    "account_dest",
    "balance_dest_before",
    "balance_dest_after",
    "is_fraud",
    "is_flagged_fraud",
    "balance_diff_orig",
    "balance_diff_dest",
    "is_large_amount",
    "is_balance_zero_out",
    "tx_type",   # 第一分区键（外层目录）
    "tx_day",    # 第二分区键（内层目录）
)

# ── 重建 DWD 表（DROP + CREATE）────────────────────────────────────────────
#   原有表列名保留 ODS 原始列名且缺少 is_balance_zero_out，需重建
print("[transform] 重建 DWD 表 ...")
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
spark.sql(f"""
CREATE TABLE {TARGET_TABLE} (
    step                 INT,
    amount               DOUBLE,
    account_orig         STRING,
    balance_orig_before  DOUBLE,
    balance_orig_after   DOUBLE,
    account_dest         STRING,
    balance_dest_before  DOUBLE,
    balance_dest_after   DOUBLE,
    is_fraud             INT,
    is_flagged_fraud     INT,
    balance_diff_orig    DOUBLE,
    balance_diff_dest    DOUBLE,
    is_large_amount      INT,
    is_balance_zero_out  INT
)
PARTITIONED BY (tx_type STRING, tx_day INT)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY")
""")

# ── 写入 ─────────────────────────────────────────────────────────────────────
print("[transform] 写入 DWD 表 ...")
(
    dwd.write
    .mode("overwrite")
    .insertInto(TARGET_TABLE)
)

# ── 验证 ─────────────────────────────────────────────────────────────────────
print("[transform] 验证输出 ...")

print("\n— 行数与欺诈数（按 tx_type）—")
spark.sql(f"""
    SELECT tx_type,
           COUNT(*)                  AS row_cnt,
           SUM(is_fraud)             AS fraud_cnt,
           SUM(is_large_amount)      AS large_cnt,
           SUM(is_balance_zero_out)  AS zero_out_cnt
    FROM {TARGET_TABLE}
    GROUP BY tx_type
    ORDER BY tx_type
""").show()

print("\n— 分区列表（前 10）—")
spark.sql(f"SHOW PARTITIONS {TARGET_TABLE}").show(10, truncate=False)

print("\n— Schema —")
spark.table(TARGET_TABLE).printSchema()

print("[transform] 完成。")
spark.stop()
