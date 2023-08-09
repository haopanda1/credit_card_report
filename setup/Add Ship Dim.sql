-- Databricks notebook source
SELECT 
  DISTINCT
  trans_store 
FROM 
  expense_report.apple_card.monthly_credit_transaction
WHERE 
  trans_store NOT IN (SELECT DISTINCT store_name_raw FROM expense_report.apple_card.shopper_dim_table)

-- COMMAND ----------

-- Store Category: Dining Out, Transportation, Grocery, Online Shopping, Personal Care, subscription, Entertainment, Rent, Travel
INSERT INTO expense_report.apple_card.shopper_dim_table VALUES
('Mercedes Benz Of Va Bc', 'Mercedes', 'Transportation', '', '757-382-2489');

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql('SELECT * FROM expense_report.apple_card.monthly_apple_installment_transaction').schema

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql import SQLContext
-- MAGIC from pyspark.sql import types as T
-- MAGIC sc = spark.sparkContext
-- MAGIC schema = T.StructType([T.StructField('col1', T.StringType(), False),T.StructField('col2', T.IntegerType(), True)])
-- MAGIC sqlContext.createDataFrame(sc.emptyRDD(), schema)

-- COMMAND ----------


