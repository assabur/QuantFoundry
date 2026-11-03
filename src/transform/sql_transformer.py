from typing import Dict
from pyspark.sql import DataFrame as SDF
from src.transform import AbstractTransform


class SqlTransformer(AbstractTransform):
    def __init__(self, sql: str, cache: bool = False) -> None:
        self.cache = cache
        self.sql = sql

    def transform(self, inputs: Dict[str, SDF], spark) -> SDF:
        print("In transform  layer ...")
        for name_view, dataframe in inputs.items():
            dataframe.createOrReplaceTempView(name_view)
        output_dataframe = spark.sql(self.sql)
        if self.cache:
            output_dataframe = output_dataframe.cache()


        return output_dataframe
