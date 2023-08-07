from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import types as T

from typing import List, Tuple, Dict

### data cleaning class - transformation & cleanning
class raw_credit_report_data_cleaning():

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