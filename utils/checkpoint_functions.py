from typing import List, Tuple, Dict
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import types as T

class checkpoint():
    def __init__(self, spark):
        self.checkpoint_file_path = r'/Volumes/expense_report/apple_card/reports/credit_card_checkpoint.txt'
        self.volumn_path = r'/Volumes/expense_report/apple_card/reports/'
        self.spark = spark

    def find_new_files(self) -> List:
        existing_files = self.__read_existing_checkpoints()
        all_files = self.__read_all_pdfs()
        return [all_file for all_file in all_files if all_file not in existing_files]

    def append_new_file(self, file_dir: str) -> None:
        existing_files = self.__read_existing_checkpoints()
        with open(self.checkpoint_file_path, 'w') as file:
            existing_files.append(file_dir)
            file.write('/n'.join(existing_files))

    def __read_existing_checkpoints(self) -> None:
        with open(self.checkpoint_file_path, 'r') as file: 
            return file.read().split('\n')
    
    def __read_all_pdfs(self) -> List:
        list_pdfs = (
            self.spark.
                sql(f"LIST '{self.volumn_path}'").
                where(
                    F.locate('pdf', F.col('path')) != 0
                ).
                select(F.col('path')).
                collect()
        )

        return [x.asDict()['path'] for x in list_pdfs]