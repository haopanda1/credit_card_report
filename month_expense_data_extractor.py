# Databricks notebook source
from PyPDF2 import PdfReader

from utils.extract_raw import raw_credit_report_data_extraction
from utils.clean_raw import raw_credit_report_data_cleaning
from utils.checkpoint_functions import checkpoint

pdf_extractor = raw_credit_report_data_extraction()
data_transformer = raw_credit_report_data_cleaning(spark)
check_func = checkpoint(spark)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ##### full script

# COMMAND ----------

for new_f in check_func.find_new_files(): 

    my_credit_report = PdfReader(new_f)

    holder_payment, holder_transaction, holder_installments =  pdf_extractor.extract(my_credit_report)

    (
        data_transformer.
            create_payment_data(holder_payment).
            write.
            saveAsTable(name='expense_report.apple_card.monthly_credit_card_payment', mode='append')
    )

    (
        data_transformer.
            create_transaction_data(holder_transaction).
            write.
            saveAsTable(name='expense_report.apple_card.monthly_credit_transaction', mode='append')
    )

    (
        data_transformer.
            create_installment_data(holder_installments).
            write.
            saveAsTable(name='expense_report.apple_card.monthly_apple_installment_transaction', mode='append')
    )

    print(f'file {new_f} processed successfully.')
    check_func.append_new_file(new_f)
    print(f'file {new_f} added to checkpoint file successfully')
