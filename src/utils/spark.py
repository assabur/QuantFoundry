import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark import SparkConf
from pyspark.sql import SparkSession


class SparkHelper:
    ICEBERG_PACKAGE = (
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0"
    )

    @staticmethod
    def setup_iceberg_config():
        warehouse = os.getenv("ICEBERG_WAREHOUSE", "./datalake/iceberg")
        if "://" not in warehouse:
            warehouse = Path(warehouse).expanduser().resolve().as_uri()
        return {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions."
                "IcebergSparkSessionExtensions"
            ),
            "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.local.type": "hadoop",
            "spark.sql.catalog.local.warehouse": warehouse,
        }

    def __init__(self, iceberg_required=False):
        self.iceberg_required = iceberg_required

    def build_config(self):
        load_dotenv()
        configs = self.setup_core_config()
        if (self.iceberg_required
                or os.getenv("ICEBERG_ENABLED", "false").lower() == "true"):
            configs["spark.jars.packages"] += "," + self.ICEBERG_PACKAGE
            configs.update(self.setup_iceberg_config())
        return configs

    def get_spark(self):
        configs = self.build_config()
        tuple_configs = [(k, v) for k, v in configs.items()]
        spark_config = SparkConf().setAll(tuple_configs)
        spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    @staticmethod
    def setup_core_config():
        return {
            "spark.sql.parquet.writeLegacyFormat": "true",
            "spark.sql.session.timeZone": "UTC",
            "spark.jars.packages": "org.postgresql:postgresql:42.7.4",
            "spark.app.name": "MyLocalApp",
            "spark.master": "local[4]",  # 4 threads
            "spark.driver.memory": "4g",  # mémoire driver
            "spark.executor.memory": "2g",  # mémoire executors
            "spark.executor.cores": "2",  # nb de cores par executor
            "spark.sql.shuffle.partitions": "8",
        }
