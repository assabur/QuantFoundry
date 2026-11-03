import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from pyspark.errors import AnalysisException

from src.extract import Extractor
from src.load import AbstractLoader, IcebergLoader
from src.skeleton import Config


def test_loader_from_yaml():
    content = Path('config/market/spark.yaml').read_text()
    config = Config.from_dict(yaml.safe_load(content))
    loader = AbstractLoader.from_config(config.outputs[0])
    assert isinstance(loader, IcebergLoader)
    assert loader.mode == 'overwrite'
    assert config.outputs[0].uri == 'local.market.trades'


@pytest.mark.parametrize('mode', ['ignore', 'upsert', 'invalid'])
def test_reject_unknown_modes(mode):
    with pytest.raises(ValueError, match='mode'):
        IcebergLoader(mode=mode)


@pytest.mark.parametrize('uri', [
    '/tmp/listing', 'silver.listing', 'local.silver.listing;DROP TABLE x',
])
def test_reject_invalid_table_identifiers(uri):
    spark = MagicMock()
    with pytest.raises(ValueError, match='identifier'):
        IcebergLoader().load(MagicMock(), uri, spark)
    spark.sql.assert_not_called()


def test_missing_partition_column_fails_before_write():
    spark = MagicMock()
    df = MagicMock(columns=['listing_id'])
    with pytest.raises(ValueError, match='partition columns'):
        IcebergLoader(partitions_fields=['eventdate']).load(
            df, 'local.silver.listing', spark
        )
    spark.sql.assert_not_called()


@pytest.mark.skipif(
    os.getenv('RUN_ICEBERG_TESTS') != '1',
    reason='Set RUN_ICEBERG_TESTS=1 with the Iceberg Spark runtime JAR',
)
def test_iceberg_roundtrip_modes_and_snapshots(tmp_path, spark):
    spark.conf.set(
        'spark.sql.catalog.icebergtest',
        'org.apache.iceberg.spark.SparkCatalog',
    )
    spark.conf.set('spark.sql.catalog.icebergtest.type', 'hadoop')
    spark.conf.set(
        'spark.sql.catalog.icebergtest.warehouse',
        (tmp_path / 'warehouse').as_uri(),
    )
    table = 'icebergtest.silver.listing'
    first = spark.createDataFrame(
        [(1, '2026-11-03')], 'id long, eventdate string'
    )
    second = spark.createDataFrame([(2, '2026-11-04')], first.schema)
    loader = IcebergLoader(mode='append', partitions_fields=['eventdate'])
    loader.load(first, table, spark)  # Bootstrap a new table.
    loader.load(second, table, spark)
    result = Extractor(format='iceberg').extract(table, spark)
    assert {row.id for row in result.collect()} == {1, 2}
    assert spark.table(table + '.snapshots').count() == 2
    with pytest.raises(AnalysisException):
        IcebergLoader(mode='error').load(first, table, spark)
    IcebergLoader(partitions_fields=['eventdate']).load(second, table, spark)
    assert [row.id for row in spark.table(table).collect()] == [2]
    partition_paths = [
        row.partition for row in spark.table(table + '.partitions').collect()
    ]
    assert len(partition_paths) == 1
    assert partition_paths[0].eventdate == '2026-11-04'
