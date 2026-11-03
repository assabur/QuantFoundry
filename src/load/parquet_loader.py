from datetime import date
from pathlib import Path
import shutil
from typing import List, Optional


class ParquetLoader:
    """Write an eventdate-partitioned dataset with the job's native backend."""

    def __init__(
        self, table_name=None, partitions_fields: Optional[List[str]] = None,
        num_partitions=None, mode='overwrite', columns=None,
    ):
        if mode not in {'overwrite', 'append', 'error', 'ignore'}:
            raise ValueError('Unsupported Parquet write mode')
        self.table_name = table_name
        self.partitions_fields = list(partitions_fields or [])
        self.num_partitions = num_partitions
        self.mode = mode
        self.columns = columns

    def load(self, df, uri, spark=None):
        import pandas as pd
        import polars as pl
        from pyspark.sql import DataFrame as SparkDataFrame
        partitions = list(dict.fromkeys(self.partitions_fields + ['eventdate']))
        if isinstance(df, SparkDataFrame):
            from pyspark.sql import functions as F
            if self.columns:
                df = df.select(self.columns)
            df = df.withColumn(
                'eventdate', F.date_format(F.current_date(), 'yyyy-MM-dd')
            )
            if self.num_partitions:
                df = df.repartition(self.num_partitions)
            df.write.mode(self.mode).partitionBy(*partitions).parquet(uri)
            return
        if not isinstance(df, (pd.DataFrame, pl.DataFrame)):
            raise TypeError('Unsupported DataFrame type')
        if '://' in uri:
            raise ValueError('Native ParquetLoader requires a local path')
        if self.num_partitions is not None:
            raise ValueError('num_partitions requires engine=spark')
        if isinstance(df, pd.DataFrame):
            df = df.loc[:, self.columns].copy() if self.columns else df.copy()
            df['eventdate'] = date.today().isoformat()
        else:
            df = df.select(self.columns) if self.columns else df
            df = df.with_columns(pl.lit(date.today().isoformat()).alias('eventdate'))
        missing = set(partitions) - set(df.columns)
        if missing:
            raise ValueError(f'Missing partition columns: {missing}')
        path = Path(uri)
        if path.exists():
            if self.mode == 'ignore':
                return
            if self.mode == 'error':
                raise FileExistsError(path)
            if self.mode == 'overwrite':
                if not path.is_dir():
                    raise NotADirectoryError(path)
                shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)
        if isinstance(df, pd.DataFrame):
            df.to_parquet(path, index=False, partition_cols=partitions)
        else:
            import pyarrow.parquet as pq
            pq.write_to_dataset(
                df.to_arrow(), root_path=path, partition_cols=partitions
            )
