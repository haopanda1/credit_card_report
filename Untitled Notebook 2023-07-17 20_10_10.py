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

from utils.clean_raw import raw_credit_report_data_cleaning

# COMMAND ----------

create_payment_data
