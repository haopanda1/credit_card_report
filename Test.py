# Databricks notebook source
import os
import re
import datetime
from PyPDF2 import PdfReader
from typing import List, Tuple, Dict

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window, WindowSpec

# COMMAND ----------

from utils.clean_raw import raw_credit_report_data_cleaning

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Define Pattern 

# COMMAND ----------

regex_payment = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n[-]?\$\d{1,1000}\.\d{1,1000}')
regex_transaction = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n\d{1}%\n[-+]?\$\d{1,1000}\.\d{1,1000}\n[-+]?\$\d{1,1000}\.\d{1,1000}')
regex_installments = re.compile(r'\n\d{2}\/\d{2}\/\d{4}\n.+\n[-+]?\$\d{1,1000}\.\d{1,1000}\s\nTRANSACTION\s.*')

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

holder_payment = []
holder_transaction = []
holder_installments = []

# COMMAND ----------

for page in range(len(my_credit_report.pages)):
    page_content = my_credit_report.pages[page].extract_text().replace(',','')

    holder_installments.append(regex_installments.findall(page_content))
    page_content = regex_installments.sub('', page_content)

    holder_transaction.append(regex_transaction.findall(page_content))
    page_content = regex_transaction.sub('', page_content)

    holder_payment.append(regex_payment.findall(page_content))
    page_content = regex_payment.sub('', page_content)

# COMMAND ----------

holder_payment = [y for x in holder_payment for y in x]
holder_transaction = [y for x in holder_transaction for y in x]
holder_installments = [y for x in holder_installments for y in x]

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


