# Databricks notebook source
import os
import re
import datetime
from PyPDF2 import PdfReader
from typing import List, Tuple, Dict

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window, WindowSpec

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

def create_payment_data(raw_payment_data: List[str]) -> DataFrame:
    raw_payment_data = [tuple(x.split('\n')[1:]) for x in raw_payment_data]
    df_payment = spark.createDataFrame(raw_payment_data, ['Date', 'Description', 'Amount'])
    df_payment = (
        df_payment.
            select(*[
                F.to_timestamp(F.col('Date'), format='MM/dd/yyyy').alias('credit_payment_date'),
                F.trim(F.col('Description')).alias('credit_payment_description'), 
                F.regexp_replace(F.col('Amount'), pattern='[-|$]', replacement='').cast(T.DecimalType(precision=10, scale=2)).alias('credit_payment_amount_usd')
            ])            
    )
    return df_payment

# COMMAND ----------

def create_transaction_data(raw_transaction_data: List[str]) -> DataFrame:
    raw_transaction_data = [tuple(x.split('\n')[1:]) for x in raw_transaction_data]
    df_transaction = spark.createDataFrame(raw_transaction_data, ['Date', 'Description', 'Daily Cash Perc', 'Daily Cash', 'Amount'])
    df_transaction = (
        df_transaction.
            select(*[
                F.to_timestamp(F.col('Date'), format='MM/dd/yyyy').alias('trans_date'),
                F.col('Description').alias('trans_store_desc'),                
                F.initcap(F.trim(
                    F.regexp_replace(
                        F.regexp_replace(
                            F.regexp_extract(F.col('Description'), '(.*?)\d', 0),
                            '\d',
                            ''
                        ),
                        '[^A-Za-z0-9\s/\.]',
                        ''
                    )
                )).alias('trans_store'),
                (F.regexp_extract(F.col('Daily Cash Perc'), '\d', 0).cast(T.DecimalType(precision=10, scale=2)) / 100).alias('trans_cash_perc'),
                F.regexp_replace(F.col('Daily Cash'), '\$', '').cast(T.DecimalType(precision=10, scale=2)).alias('trans_cashback_amount'),
                F.regexp_replace(F.col('Amount'), '\$', '').cast(T.DecimalType(precision=10, scale=2)).alias('trans_cash_amount')
            ])
    )
    return df_transaction

# COMMAND ----------

def create_installment_data(raw_installment_data: List[str]) -> DataFrame:
    raw_installment_data = [tuple(x.split('\n')[1:]) for x in raw_installment_data]
    df_transaction = spark.createDataFrame(raw_installment_data, ['Date', 'Apple_Installment_Loc','Amount', 'Transaction_ID'])   
    df_transaction = (
        df_transaction.
            select(
                F.to_timestamp(F.col('Date'), format='MM/dd/yyyy').alias('transaction_date'),
                F.col('Apple_Installment_Loc').alias('apple_installment_loc'),
                F.regexp_extract(F.col('Transaction_ID'), r'[1-9]\w+', 0).alias('transaction_id'),
                F.regexp_replace(F.col('Amount'), '\$', '').cast(T.DecimalType(precision=10, scale=2)).alias('transaction_amount')
        )
    )
    return df_transaction

# COMMAND ----------

display(create_transaction_data(holder_transaction))

# COMMAND ----------


