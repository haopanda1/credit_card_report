# Databricks notebook source
import os

# COMMAND ----------

folder = r'dbfs:/Volumes/expense_report/apple_card/reports'

# COMMAND ----------

os.listdir(r'/dbfs/Volumes/expense_report/apple_card/reports')

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC LIST 'dbfs:/Volumes/expense_report/apple_card/reports';

# COMMAND ----------

os.listdir(r'/Volumes/expense_report/apple_card/reports')

# COMMAND ----------

dbutils.fs.ls('/Volumes/expense_report/apple_card/reports')

# COMMAND ----------


