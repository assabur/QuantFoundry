import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional
from urllib.parse import unquote, urlsplit

from src.load.abstract_loader import AbstractLoader


class FileLoader(AbstractLoader):
    """Write native files or distributed Spark datasets, without conversion."""

    def __init__(
        self,
        format: str = "parquet",
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        if format not in {"parquet", "csv", "json", "ndjson"}:
            raise ValueError("format must be parquet, csv, json or ndjson")
        if mode not in {"overwrite", "error", "ignore"}:
            raise ValueError("mode must be overwrite, error or ignore")
        self.format = format
        self.mode = mode
        self.options = dict(options or {})

    def load(self, df, uri: str, spark=None) -> None:
        from pyspark.sql import DataFrame as SparkDataFrame
        if isinstance(df, SparkDataFrame):
            format = 'json' if self.format == 'ndjson' else self.format
            mode = 'errorifexists' if self.mode == 'error' else self.mode
            df.write.mode(mode).format(format).options(**self.options).save(uri)
            return
        parsed = urlsplit(uri)
        if parsed.scheme and (
            parsed.scheme != "file" or parsed.netloc not in {"", "localhost"}
        ):
            raise ValueError(
                "Only local paths and local file:// URIs supported"
            )
        path = Path(unquote(parsed.path) if parsed.scheme else uri)
        if path.exists():
            if path.is_dir():
                raise IsADirectoryError(path)
            if self.mode == "ignore":
                return
            if self.mode == "error":
                raise FileExistsError(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Publish only after a successful conversion and write.
        with NamedTemporaryFile(dir=path.parent, delete=False) as output:
            temporary = Path(output.name)
            try:
                self._write(df, output)
                output.close()
                if self.mode == "overwrite":
                    os.replace(temporary, path)
                else:
                    try:
                        os.link(temporary, path)
                    except FileExistsError:
                        if self.mode == "error":
                            raise
            finally:
                temporary.unlink(missing_ok=True)

    def _write(self, df, output) -> None:
        import pandas as pd
        import polars as pl
        options = dict(self.options)
        if isinstance(df, pd.DataFrame):
            if self.format == 'parquet':
                options.setdefault('index', False)
                df.to_parquet(output, **options)
            elif self.format == 'csv':
                options.setdefault('index', False)
                df.to_csv(output, **options)
            elif not (self.format == 'ndjson' and df.empty):
                options.setdefault('orient', 'records')
                options.setdefault('date_format', 'iso')
                options.setdefault('lines', self.format == 'ndjson')
                df.to_json(output, **options)
        elif isinstance(df, pl.DataFrame):
            methods = {
                'parquet': df.write_parquet, 'csv': df.write_csv,
                'json': df.write_json, 'ndjson': df.write_ndjson,
            }
            methods[self.format](output, **options)
        else:
            raise TypeError('Expected a native pandas or Polars DataFrame')
