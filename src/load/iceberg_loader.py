import re
from typing import List, Optional

from pyspark.sql import DataFrame

from src.load.abstract_loader import AbstractLoader


class IcebergLoader(AbstractLoader):
    """Write a catalog table using Spark's DataFrameWriterV2 API."""

    def __init__(
        self,
        mode: str = "overwrite",
        partitions_fields: Optional[List[str]] = None,
    ) -> None:
        if mode not in {"overwrite", "append", "error"}:
            raise ValueError("Iceberg mode must be overwrite, append or error")
        self.mode = mode
        self.partitions_fields = list(partitions_fields or [])

    def load(self, df: DataFrame, uri: str, spark) -> None:
        # A fully qualified identifier avoids writing to the session catalog.
        parts = uri.split(".")
        if len(parts) < 3 or any(
            not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part)
            for part in parts
        ):
            raise ValueError(
                "Iceberg uri must be a catalog.namespace.table identifier"
            )
        missing = set(self.partitions_fields) - set(df.columns)
        if missing:
            raise ValueError(f"Missing Iceberg partition columns: {missing}")

        table = ".".join(f"`{part}`" for part in parts)
        namespace = ".".join(f"`{part}`" for part in parts[:-1])
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

        writer = df.writeTo(table).using("iceberg")
        if self.mode == "append" and spark.catalog.tableExists(table):
            writer.append()
            return

        writer = writer.tableProperty("format-version", "2")
        if self.partitions_fields:
            writer = writer.partitionedBy(*self.partitions_fields)
        if self.mode == "overwrite":
            writer.createOrReplace()
        else:
            writer.create()
