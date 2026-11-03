"""Backends use native DataFrames throughout each job, without fallback."""
import importlib
from abc import ABC, abstractmethod

from src.engines.operations import apply_operations


class ComputeEngine(ABC):
    name = None
    spark = None

    def start(self):
        pass

    def close(self):
        pass

    def check_config(self, config):
        pass

    @abstractmethod
    def extract(self, config):
        pass

    @abstractmethod
    def row_count(self, df):
        pass

    def transform(self, config, inputs):
        kind = config.transformer.rsplit('.', 1)[-1]
        params = config.parameters or {}
        if kind == 'DataFrameTransformer':
            return apply_operations(inputs, params['operations'], self.name)
        if kind == 'SqlTransformer' and self.name == 'polars':
            import polars as pl
            context = pl.SQLContext(**(inputs or {}))
            return context.execute(params['sql'], eager=True)
        if kind == 'FreeStyleTransformer':
            name = params['function_name']
            if '.' not in name:
                raise ValueError('Native functions need a dotted function_name')
            module, function = name.rsplit('.', 1)
            return getattr(importlib.import_module(module), function)(
                inputs, params.get('function_parameters')
            )
        raise ValueError(f'{kind} is not supported by engine={self.name}')

    def validate(self, config, df):
        from src.engines.quality import validate_native
        return validate_native(self.name, config, df)

    def load(self, config, df):
        from src.load import AbstractLoader
        return AbstractLoader.from_config(config).load(
            df, config.uri, self.spark
        )


class NativeEngine(ComputeEngine):
    def check_config(self, config):
        for source in config.inputs or []:
            if source.extractor != 'src.extract.Extractor':
                raise ValueError('Custom extractor requires engine=spark')
            params = source.parameters or {}
            if params.get('ddl_schema') or params.get('filter_on'):
                raise ValueError(
                    'Use native reader options and DataFrameTransformer '
                    'instead of ddl_schema/filter_on for native jobs'
                )
        for transform in config.transforms or []:
            kind = transform.transformer.rsplit('.', 1)[-1]
            allowed = {'DataFrameTransformer', 'FreeStyleTransformer'}
            if self.name == 'polars':
                allowed.add('SqlTransformer')
            if kind not in allowed:
                raise ValueError(
                    f'{kind} is not supported by engine={self.name}'
                )
            if kind == 'FreeStyleTransformer':
                name = (transform.parameters or {}).get('function_name', '')
                if '.' not in name:
                    raise ValueError(
                        'Use a dotted native function or DataFrameTransformer'
                    )
        for output in config.outputs or []:
            if output.loader not in {
                'src.load.FileLoader', 'src.load.ParquetLoader',
            }:
                raise ValueError(
                    f'{output.loader} requires engine=spark; '
                    'use FileLoader for native files'
                )
        for check in config.quality_gate or []:
            if check.validator != 'src.validators.CoreValidator':
                raise ValueError('Native engines support CoreValidator only')

    def extract(self, config):
        params = dict(config.parameters or {})
        format = params.pop('format', 'csv')
        options = params.pop('options', {}) or {}
        columns = params.pop('columns', None)
        if params:
            raise ValueError(f'Unsupported input parameters: {sorted(params)}')
        if self.name == 'pandas':
            import pandas as pd
            readers = {
                'csv': pd.read_csv, 'parquet': pd.read_parquet,
                'json': pd.read_json, 'ndjson': pd.read_json,
            }
        else:
            import polars as pl
            readers = {
                'csv': pl.read_csv, 'parquet': pl.read_parquet,
                'json': pl.read_json, 'ndjson': pl.read_ndjson,
            }
        if format not in readers:
            raise ValueError(f'Unsupported {self.name} input format: {format}')
        if format == 'ndjson' and self.name == 'pandas':
            options = {'lines': True, **options}
        df = readers[format](config.uri, **options)
        if columns:
            names = columns['list']
            names = [names] if isinstance(names, str) else names
            op = 'select' if columns['type'] == 'include' else 'drop'
            df = apply_operations(
                df, [{'op': op, 'columns': names}], self.name
            )
        return df

    def row_count(self, df):
        if self.name == 'pandas':
            import pandas as pd
            expected = pd.DataFrame
        else:
            import polars as pl
            expected = pl.DataFrame
        if not isinstance(df, expected):
            raise TypeError(f'engine={self.name} requires a native DataFrame')
        return len(df) if self.name == 'pandas' else df.height


class PandasEngine(NativeEngine):
    name = 'pandas'


class PolarsEngine(NativeEngine):
    name = 'polars'


class SparkEngine(ComputeEngine):
    name = 'spark'

    def __init__(self, spark=None, iceberg_required=False):
        self.spark = spark
        self.cached = []
        self.iceberg_required = iceberg_required
        self.iceberg_catalogs = set()

    @staticmethod
    def requires_iceberg(config):
        return any(
            output.loader == 'src.load.IcebergLoader'
            for output in config.outputs or []
        ) or any(
            (source.parameters or {}).get('format') == 'iceberg'
            for source in config.inputs or []
        )

    def check_config(self, config):
        self.iceberg_required |= self.requires_iceberg(config)
        uris = [output.uri for output in config.outputs or []
                if output.loader == 'src.load.IcebergLoader']
        uris += [source.uri for source in config.inputs or []
                 if (source.parameters or {}).get('format') == 'iceberg']
        self.iceberg_catalogs.update(
            uri.split('.')[0] for uri in uris if isinstance(uri, str)
        )

    def start(self):
        if self.spark is None:
            from src.utils.spark import SparkHelper
            self.spark = SparkHelper(
                iceberg_required=self.iceberg_required,
            ).get_spark()
        for catalog in self.iceberg_catalogs:
            if not self.spark.conf.get(f'spark.sql.catalog.{catalog}', None):
                raise RuntimeError(
                    f'Iceberg catalog {catalog!r} is not configured. Restart '
                    'Spark with the Iceberg runtime and catalog configured; '
                    'SparkHelper(iceberg_required=True).get_spark() sets up local.'
                )

    def extract(self, config):
        from src.extract import AbstractExtractor
        return AbstractExtractor.from_config(config).extract(
            config.uri, self.spark
        )

    def transform(self, config, inputs):
        if config.transformer.rsplit('.', 1)[-1] == 'DataFrameTransformer':
            return super().transform(config, inputs)
        from src.transform import AbstractTransform
        return AbstractTransform.from_config(config).transform(
            inputs, self.spark
        )

    def validate(self, config, df):
        from src.validators import AbstractValidator
        return AbstractValidator.from_config(config).validate(df, self.spark)

    def row_count(self, df):
        # Execute lazy Spark work inside stage timings and retain the result.
        if not df.is_cached:
            df.persist()
            self.cached.append(df)
        return df.count()

    def close(self):
        for df in self.cached:
            df.unpersist()
        self.cached.clear()


def create_engine(name, spark=None):
    engines = {
        'spark': SparkEngine, 'pandas': PandasEngine, 'polars': PolarsEngine,
    }
    if name not in engines:
        raise ValueError(f'Unknown engine {name!r}; use spark, pandas or polars')
    return SparkEngine(spark) if name == 'spark' else engines[name]()
