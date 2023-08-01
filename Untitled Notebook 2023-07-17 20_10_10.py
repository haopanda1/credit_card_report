# Databricks notebook source
import os

# COMMAND ----------

# abfss://<container_name>@<storage_acount_name>.dfs.core.windows.net/<remaining_file_path>

# COMMAND ----------

adls_file_path = r'abfss://csvlake@datalakegen2hao.dfs.core.windows.net/credit_card_report/Apple Card Statement - May 2023.pdf'
target_file_path = r'dbfs:/FileStore/my_expense_report_apple/Apple Card Statement - May 2023.pdf'
access_target_file_path = r'/dbfs/FileStore/my_expense_report_apple/Apple Card Statement - May 2023.pdf'

# COMMAND ----------

adls_file_path = 'abfss://<container_name>@<storage_acount_name>.dfs.core.windows.net/<remaining_file_path>'
target_file_path = r'dbfs:/<folder_name>/<file_name>'
access_target_file_path = r'/dbfs/<folder_name>/<file_name>'

# COMMAND ----------

dbutils.fs.ls(adls_file_path)

# COMMAND ----------

os.path.exists(adls_file_path)

# COMMAND ----------

with open(adls_file_path, 'rb') as file:
    print(file.read())

# COMMAND ----------

# dbutils.fs.cp(adls_file_path, target_file_path)

# COMMAND ----------

# Return specified bytes of data from file
dbutils.fs.head('dbfs:/my_file_path', 100000)
# List specified directory
dbutils.fs.ls('dbfs:/my_file_path')
# remove folder / directory. When True, remove non empty folder
dbutils.fs.rm('dbfs:/my_file_path', True)

# COMMAND ----------

pyspark.read.format('csv').load('dbfs:/my_file_path')

# COMMAND ----------

dbutils.fs.ls('dbfs:/FileStore/my_expense_report_apple')

# COMMAND ----------

os.path.exists(access_target_file_path)

# COMMAND ----------

with open(access_target_file_path, 'rb') as file:
    print(file.read())

# COMMAND ----------


