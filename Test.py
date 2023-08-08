# Databricks notebook source
from PyPDF2 import PdfReader

from utils.extract_raw import raw_credit_report_data_extraction
from utils.clean_raw import raw_credit_report_data_cleaning
from utils.checkpoint_functions import checkpoint

# COMMAND ----------

pdf_extractor = raw_credit_report_data_extraction()
data_transformer = raw_credit_report_data_cleaning(spark)
check_func = checkpoint()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import types as T

# COMMAND ----------

new_files = check_func.find_new_files()

for new_f in new_files: 
    

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Check New File and Write to CheckPoint

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ###### find new files

# COMMAND ----------

new_files = [all_file for all_file in list_pdfs if all_file not in checkpoints]
print(new_files)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ###### write new files to checkpoint file

# COMMAND ----------

with open(checkpoint_file_path, 'w') as file:
    file.write('/n'.join(list_pdfs))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Access PDF

# COMMAND ----------

access_target_file_path = r'/Volumes/expense_report/apple_card/reports/Apple Card Statement - May 2023.pdf'

# COMMAND ----------

my_credit_report = PdfReader(access_target_file_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Extract

# COMMAND ----------

holder_payment, holder_transaction, holder_installments =  pdf_extractor().extract(my_credit_report)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### Clean and Make DF

# COMMAND ----------

df_payment = data_transformer.create_payment_data(holder_payment)
df_transaction = data_transformer.create_transaction_data(holder_transaction)
df_installment = data_transformer.create_installment_data(holder_installments)

# COMMAND ----------

display(df_installment)

# COMMAND ----------

df_installment.printSchema()

# COMMAND ----------


