-- ============================================================
-- ODS Layer: raw_transactions
-- ============================================================
CREATE DATABASE IF NOT EXISTS paysim;

CREATE EXTERNAL TABLE IF NOT EXISTS paysim.raw_transactions (
    step        INT,
    type        STRING,
    amount      DOUBLE,
    nameOrig    STRING,
    oldbalanceOrg  DOUBLE,
    newbalanceOrig DOUBLE,
    nameDest    STRING,
    oldbalanceDest DOUBLE,
    newbalanceDest DOUBLE,
    isFraud        INT,
    isFlaggedFraud INT
)
PARTITIONED BY (tx_type STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/paysim/raw'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ============================================================
-- DWD Layer: dwd_transactions
-- ============================================================
CREATE TABLE IF NOT EXISTS paysim.dwd_transactions (
    step              INT,
    amount            DOUBLE,
    nameOrig          STRING,
    oldbalanceOrg     DOUBLE,
    newbalanceOrig    DOUBLE,
    nameDest          STRING,
    oldbalanceDest    DOUBLE,
    newbalanceDest    DOUBLE,
    isFraud           INT,
    isFlaggedFraud    INT,
    balance_diff_orig DOUBLE,
    balance_diff_dest DOUBLE,
    is_large_transfer INT
)
PARTITIONED BY (tx_type STRING, tx_day INT)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ============================================================
-- ADS Layer: ads_fraud_summary
-- ============================================================
CREATE TABLE IF NOT EXISTS paysim.ads_fraud_summary (
    tx_type     STRING,
    tx_day      INT,
    total_count BIGINT,
    fraud_count BIGINT,
    fraud_rate  DOUBLE,
    total_amount DOUBLE
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');

-- ============================================================
-- ADS Layer: ads_high_risk_accounts
-- ============================================================
CREATE TABLE IF NOT EXISTS paysim.ads_high_risk_accounts (
    nameOrig       STRING,
    total_transfer_amount DOUBLE,
    tx_count       BIGINT,
    zero_balance_count BIGINT,
    fraud_count    BIGINT
)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY');
