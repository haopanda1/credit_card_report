from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import types as T
from pyspark.sql import SQLContext

from typing import List, Tuple, Dict

### data cleaning class - transformation & cleanning
class raw_credit_report_data_cleaning():
    def __init__(self, spark):
        self.spark = spark

        self.payment_schema = T.StructType(
            [
                T.StructField('credit_payment_date', T.TimestampType(), True), 
                T.StructField('credit_payment_description', T.StringType(), True), 
                T.StructField('credit_payment_amount_usd', T.DecimalType(10,2), True)
            ]
        )

        self.transaction_schema = T.StructType(
            [
                T.StructField('trans_date', T.TimestampType(), True), 
                T.StructField('trans_store_desc', T.StringType(), True), 
                T.StructField('trans_store', T.StringType(), True), 
                T.StructField('trans_cash_perc', T.DecimalType(14,6), True), 
                T.StructField('trans_cashback_amount', T.DecimalType(10,2), True), 
                T.StructField('trans_cash_amount', T.DecimalType(10,2), True)
            ]
        ) 

        self.installment_schema = T.StructType(
            [
                T.StructField('transaction_date', T.TimestampType(), True), 
                T.StructField('apple_installment_loc', T.StringType(), True), 
                T.StructField('transaction_id', T.StringType(), True), 
                T.StructField('transaction_amount', T.sdDecimalType(10,2), True)
            ]
        )

    def create_payment_data(self, raw_payment_data: List[str]) -> DataFrame:
        try: 
            raw_payment_data = [tuple(x.split('\n')[1:]) for x in raw_payment_data]
            df_payment = self.spark.createDataFrame(raw_payment_data, ['Date', 'Description', 'Amount'])
            df_payment = (
                df_payment.
                    select(*[
                        F.to_timestamp(F.col('Date'), format='MM/dd/yyyy').alias('credit_payment_date'),
                        F.trim(F.col('Description')).alias('credit_payment_description'), 
                        F.regexp_replace(F.col('Amount'), pattern='[-|$]', replacement='').cast(T.DecimalType(precision=10, scale=2)).alias('credit_payment_amount_usd')
                    ])            
            )
            return df_payment
        except ValueError:
            return sqlContext.createDataFrame(spark.sparkContext.emptyRDD(), self.payment_schema)

    def create_transaction_data(self, raw_transaction_data: List[str]) -> DataFrame:
        try: 
            raw_transaction_data = [tuple(x.split('\n')[1:]) for x in raw_transaction_data]
            df_transaction = self.spark.createDataFrame(raw_transaction_data, ['Date', 'Description', 'Daily Cash Perc', 'Daily Cash', 'Amount'])
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
        except ValueError:
            return sqlContext.createDataFrame(spark.sparkContext.emptyRDD(), self.payment_schema)

    def create_installment_data(self, raw_installment_data: List[str]) -> DataFrame:
        try: 
            raw_installment_data = [tuple(x.split('\n')[1:]) for x in raw_installment_data]
            df_transaction = self.spark.createDataFrame(raw_installment_data, ['Date', 'Apple_Installment_Loc','Amount', 'Transaction_ID'])   
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
        except ValueError:
            return sqlContext.createDataFrame(spark.sparkContext.emptyRDD(), self.payment_schema)