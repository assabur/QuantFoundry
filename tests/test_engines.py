import json
import logging
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import polars as pl
import pytest
import yaml

from src.engines import create_engine
from src.pipeline import Pipeline
from src.skeleton import Config, GroupConfig
from src.wrapper_pipeline import WrapperPipeline


OPERATIONS = [
    {'op': 'filter', 'column': 'id', 'operator': 'gt', 'value': 0},
    {'op': 'fill_null', 'values': {'city': 'Unknown'}},
    {'op': 'unique', 'columns': ['id']},
    {'op': 'drop', 'columns': ['id']},
    {'op': 'rename', 'mapping': {'price': 'amount'}},
    {'op': 'group_by', 'columns': ['city'], 'aggregations': {'amount': 'sum'}},
    {'op': 'select', 'columns': ['city', 'amount']},
    {'op': 'sort', 'columns': ['city']},
]


def job(tmp_path, engine, name='ingestion'):
    source = tmp_path / 'source.csv'
    source.write_text('id,city,price\n1,Paris,10\n2,Lyon,20\n-1,X,99\n3,,5\n3,,5\n')
    options = {'header': True, 'inferSchema': True} if engine == 'spark' else {}
    return Config.from_dict({
        'name': name, 'engine': engine,
        'inputs': [{'name': 'source', 'uri': str(source),
                    'parameters': {'format': 'csv', 'options': options}}],
        'transforms': [{
            'name': 'result', 'transformer': 'DataFrameTransformer',
            'inputs': 'source', 'parameters': {'operations': OPERATIONS},
        }],
        'outputs': [{
            'name': 'out', 'input': 'result', 'loader': 'src.load.FileLoader',
            'uri': str(tmp_path / f'{name}-{engine}.parquet'),
            'parameters': {'format': 'parquet'},
        }],
    })


def read_result(config):
    uri = config.outputs[0].uri
    return pd.read_parquet(uri).sort_values('city').to_dict('records')


EXPECTED = [
    {'city': 'Lyon', 'amount': 20},
    {'city': 'Paris', 'amount': 10},
    {'city': 'Unknown', 'amount': 5},
]


@pytest.mark.parametrize('name', ['pandas', 'polars'])
def test_native_ingestion_never_starts_spark(name, tmp_path, monkeypatch, caplog):
    from src.utils.spark import SparkHelper
    from pyspark.sql import DataFrame

    def forbid(*args, **kwargs):
        raise AssertionError('Native jobs must not use Spark')

    monkeypatch.setattr(SparkHelper, 'get_spark', forbid)
    monkeypatch.setattr(DataFrame, 'toPandas', forbid)
    config = job(tmp_path, name)
    pipeline = Pipeline(config)
    with caplog.at_level(logging.INFO):
        frames = pipeline.run()
    expected_type = pd.DataFrame if name == 'pandas' else pl.DataFrame
    assert isinstance(frames['source'], expected_type)
    assert isinstance(frames['result'], expected_type)
    assert read_result(config) == EXPECTED
    metrics = [json.loads(r.message) for r in caplog.records
               if r.name == 'quantfoundry.ingestion']
    assert len(metrics) == 1
    report = metrics[0]
    assert report['engine'] == name and report['status'] == 'success'
    assert report['input_rows'] == 5 and report['output_rows'] == 3
    assert report['duration_s'] >= report['engine_setup_s'] >= 0
    assert report['input_rows_per_second'] > 0
    assert [s['stage'] for s in report['stages']] == ['extract', 'transform', 'load']
    assert [s['rows'] for s in report['stages']] == [5, 3, 3]
    assert all(s['duration_s'] >= 0 for s in report['stages'])


def test_spark_matches_native_result_and_metrics(tmp_path, spark):
    config = job(tmp_path, 'spark')
    pipeline = Pipeline(config, spark=spark)
    frames = pipeline.run()
    assert read_result(config) == EXPECTED
    assert pipeline.metrics['input_rows'] == 5
    assert pipeline.metrics['output_rows'] == 3
    assert not frames['result'].is_cached
    assert not frames['source'].is_cached


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
@pytest.mark.parametrize('format', ['csv', 'json', 'ndjson', 'parquet'])
def test_file_read_and_write_without_conversion(tmp_path, engine, format):
    from src.load import FileLoader
    from src.skeleton import InputConfig
    df = pd.DataFrame({'id': [1], 'city': ['Évry']})
    if engine == 'polars':
        df = pl.from_pandas(df)
    path = tmp_path / f'file.{format}'
    FileLoader(format=format).load(df, path.as_uri())
    backend = create_engine(engine)
    source = InputConfig(name='file', uri=str(path), parameters={'format': format})
    result = backend.extract(source)
    rows = result.to_dict('records') if engine == 'pandas' else result.to_dicts()
    assert rows == [{'id': 1, 'city': 'Évry'}]


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_file_modes_and_atomic_failure(tmp_path, engine):
    from src.load import FileLoader
    df = pd.DataFrame({'id': [1]})
    df = df if engine == 'pandas' else pl.from_pandas(df)
    path = tmp_path / 'out.csv'
    FileLoader(format='csv').load(df, str(path))
    original = path.read_bytes()
    with pytest.raises(FileExistsError):
        FileLoader(format='csv', mode='error').load(df, str(path))
    FileLoader(format='csv', mode='ignore').load(None, str(path))
    with pytest.raises(TypeError):
        FileLoader(format='csv').load(object(), str(path))
    assert path.read_bytes() == original
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_native_partitioned_parquet(tmp_path, engine):
    from src.load import ParquetLoader
    frame = pd.DataFrame({'city': ['Paris'], 'price': [10]})
    if engine == 'polars':
        frame = pl.from_pandas(frame)
    path = tmp_path / 'dataset'
    ParquetLoader().load(frame, str(path))
    assert list(path.glob('eventdate=*/*.parquet'))
    assert pd.read_parquet(path)['city'].tolist() == ['Paris']


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
@pytest.mark.parametrize('rule,expected', [
    ('expect_column_values_to_not_be_null', 1),
    ('expect_column_values_to_be_unique', 2),
    ('expect_column_values_to_match_regex', 1),
])
def test_native_quality_reports(tmp_path, monkeypatch, engine, rule, expected):
    from src.skeleton import ValidationConfig
    monkeypatch.setenv('SILVER', str(tmp_path))
    df = pd.DataFrame({'city': ['Paris', 'Paris', 'Lyon', None]})
    if engine == 'polars':
        df = pl.from_pandas(df)
    config = ValidationConfig(
        name='quality', validator='CoreValidator', input='data',
        parameters={'rule': rule, 'columns': ['city'],
                    'rule_kwargs': {'regex': '^Paris$'}},
    )
    events = create_engine(engine).validate(config, df)
    assert events[0]['unexpected_count'] == expected
    assert not events[0]['success']
    assert list(tmp_path.rglob('*.ndjson'))


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_failure_is_measured_and_logged(tmp_path, engine, caplog):
    config = job(tmp_path, engine)
    config.inputs[0].uri = str(tmp_path / 'missing.csv')
    pipeline = Pipeline(config)
    with caplog.at_level(logging.INFO), pytest.raises(FileNotFoundError):
        pipeline.run()
    assert pipeline.metrics['status'] == 'failed'
    assert pipeline.metrics['error_type'] == 'FileNotFoundError'
    assert pipeline.metrics['stages'][0]['status'] == 'failed'
    assert pipeline.metrics['duration_s'] > 0
    assert any('ingestion_metrics' in r.message for r in caplog.records)


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_spark_only_loader_fails_before_read(tmp_path, engine):
    config = job(tmp_path, engine)
    config.inputs[0].uri = 'does-not-exist.csv'
    config.outputs[0].loader = 'src.load.IcebergLoader'
    pipeline = Pipeline(config)
    with pytest.raises(ValueError, match='requires engine=spark'):
        pipeline.run()
    assert pipeline.metrics['status'] == 'failed'
    assert pipeline.metrics['stages'] == []


def test_sql_capability(tmp_path):
    from src.skeleton import TransformConfig
    config = TransformConfig(
        name='sql', transformer='SqlTransformer', inputs={'data': 'source'},
        parameters={'sql': 'SELECT SUM(value) AS total FROM data'},
    )
    result = create_engine('polars').transform(
        config, {'data': pl.DataFrame({'value': [10, 20]})}
    )
    assert result.to_dicts() == [{'total': 30}]
    with pytest.raises(ValueError, match='not supported'):
        create_engine('pandas').transform(config, {})


def test_mixed_jobs_and_failure_continue(tmp_path, spark):
    configs = [job(tmp_path, e, name=e) for e in ['pandas', 'polars', 'spark']]
    bad = job(tmp_path, 'pandas', name='failed')
    bad.inputs[0].uri = 'missing.csv'
    wrapper = WrapperPipeline(GroupConfig(configs=[bad, *configs]), spark=spark)
    reports = wrapper.launch()
    assert [s[2] for s in wrapper.status] == ['Fails', 'Success', 'Success', 'Success']
    assert len(wrapper.errors) == 1
    assert [r['engine'] for r in reports] == ['pandas', 'pandas', 'polars', 'spark']
    assert all(read_result(c) == EXPECTED for c in configs)


def test_unknown_engine_logged(tmp_path):
    config = job(tmp_path, 'unknown')
    pipeline = Pipeline(config)
    with pytest.raises(ValueError, match='Unknown engine'):
        pipeline.run()
    assert pipeline.metrics['engine'] == 'unknown'
    assert pipeline.metrics['status'] == 'failed'


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_cli_without_java(tmp_path, engine):
    config = job(tmp_path, engine)
    spec = tmp_path / 'job.yaml'
    spec.write_text(yaml.safe_dump(config.to_dict()))
    completed = subprocess.run(
        [sys.executable, 'local.py', '--config', str(spec)],
        env={**os.environ, 'JAVA_HOME': '/not-installed'},
        capture_output=True, text=True, timeout=40,
    )
    assert completed.returncode == 0, completed.stderr
    logs = [json.loads(line) for line in completed.stderr.splitlines()
            if line.startswith('{') and 'ingestion_metrics' in line]
    assert len(logs) == 1 and logs[0]['engine'] == engine
    assert logs[0]['input_rows'] == 5
    assert read_result(config) == EXPECTED


def test_legacy_config_defaults_to_spark():
    assert Config.from_dict({'name': 'legacy'}).engine == 'spark'


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_native_custom_result_cannot_change_engine(engine):
    wrong = (pl.DataFrame({'id': [1]}) if engine == 'pandas'
             else pd.DataFrame({'id': [1]}))
    with pytest.raises(TypeError, match='native DataFrame'):
        create_engine(engine).row_count(wrong)


def native_head(inputs, parameters):
    return inputs.head(parameters['rows'])


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_native_custom_function(tmp_path, engine):
    from src.skeleton import TransformConfig
    frame = pd.DataFrame({'id': [1, 2]})
    if engine == 'polars':
        frame = pl.from_pandas(frame)
    config = TransformConfig(
        name='custom', transformer='FreeStyleTransformer', inputs='data',
        parameters={'function_name': __name__ + '.native_head',
                    'function_parameters': {'rows': 1}},
    )
    backend = create_engine(engine)
    result = backend.transform(config, frame)
    assert backend.row_count(result) == 1


def test_failed_cli_returns_nonzero_and_metrics(tmp_path):
    config = job(tmp_path, 'pandas')
    config.inputs[0].uri = str(tmp_path / 'missing.csv')
    spec = tmp_path / 'failed.yaml'
    spec.write_text(yaml.safe_dump(config.to_dict()))
    completed = subprocess.run(
        [sys.executable, 'local.py', '--config', str(spec)],
        capture_output=True, text=True, timeout=40,
    )
    assert completed.returncode == 1
    logs = [json.loads(line) for line in completed.stderr.splitlines()
            if line.startswith('{') and 'ingestion_metrics' in line]
    assert len(logs) == 1 and logs[0]['status'] == 'failed'
