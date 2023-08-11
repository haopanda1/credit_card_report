-- Databricks notebook source
-- MAGIC %md 
-- MAGIC
-- MAGIC ##### Identify Similar Records

-- COMMAND ----------

SELECT * FROM expense_report.apple_card.shopper_dim_table
WHERE store_name_raw LIKE '%Usps%'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ##### Identify Records to Update

-- COMMAND ----------

SELECT 
  DISTINCT
  trans_store 
FROM 
  expense_report.apple_card.monthly_credit_transaction
WHERE 
  trans_store NOT IN (SELECT DISTINCT store_name_raw FROM expense_report.apple_card.shopper_dim_table)
ORDER BY 
  trans_store

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ##### Update Records

-- COMMAND ----------

-- Store Category: Dining Out, Transportation, Grocery, Online Shopping, Personal Care, subscription, Entertainment, Rent, Travel, Other
INSERT INTO expense_report.apple_card.shopper_dim_table VALUES
('Uber Eats', 'Uber Eats', 'Dining Out', '', ''),
('Uniqlo Pike Rose', 'Uniqlo', 'Online Shopping', '', ''),
('Usps Change Of Address', 'USPS', 'Online Shopping', '', ''),
('Vans', 'Vans', 'Online Shopping', '', ''),
('Virginia Tire Elden', 'Virginia Tire Elden', 'Transportation', '', ''),
('Westin Atlanta Airport', 'Westin Atlanta Airport', 'Travel', '', ''),
('Wholefds Umd', 'Whole Foods Market', 'Grocery', '', ''),
('Zen Cafe Treats', 'Zen Cafe Treats', 'Dining Out', '', ''),
('', '', 'Other', '', '')

;

-- COMMAND ----------


