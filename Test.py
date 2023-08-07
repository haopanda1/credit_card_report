# Databricks notebook source
import os
import re
import datetime
from PyPDF2 import PdfReader
from typing import List, Tuple, Dict

# COMMAND ----------

from utils.extract_raw import raw_credit_report_data_extraction
from utils.clean_raw import raw_credit_report_data_cleaning

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Move ADLS File to DBFS

# COMMAND ----------

access_target_file_path = r'/Volumes/expense_report/apple_card/reports/Apple Card Statement - May 2023.pdf'

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Access PDF

# COMMAND ----------

my_credit_report = PdfReader(access_target_file_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Metadata

# COMMAND ----------

meta_creator = my_credit_report.metadata.creator
meta_creation_date_raw = my_credit_report.metadata.creation_date_raw
meta_title = my_credit_report.metadata.title

# COMMAND ----------

print(meta_creator)
print(meta_creation_date_raw)
print(meta_title)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Extract

# COMMAND ----------

holder_payment, holder_transaction, holder_installments =  raw_credit_report_data_extraction().extract(my_credit_report)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Clean and Make DF

# COMMAND ----------

data_transformer = raw_credit_report_data_cleaning(spark)
df_payment = data_transformer.create_payment_data(holder_payment)
df_transaction = data_transformer.create_transaction_data(holder_transaction)
df_installment = data_transformer.create_installment_data(holder_installments)

# COMMAND ----------


