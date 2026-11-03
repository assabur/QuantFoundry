"""Regressions for Iceberg jobs launched without ICEBERG_ENABLED."""
from unittest.mock import MagicMock

import pytest

from src.engines.backend import SparkEngine
from src.pipeline import Pipeline
from src.skeleton import Config, GroupConfig
from src.wrapper_pipeline import WrapperPipeline


def iceberg_job():
    return Config.from_dict({
        'name': 'iceberg', 'engine': 'spark',
        'outputs': [{'name': 'out', 'input': 'data',
                     'uri': 'local.market.trades',
                     'loader': 'src.load.IcebergLoader'}],
    })


def test_output_requires_iceberg_before_session_start(monkeypatch):
    from src.utils.spark import SparkHelper
    required = []
    session = MagicMock()
    session.conf.get.return_value = 'org.apache.iceberg.spark.SparkCatalog'

    def start(helper):
        required.append(helper.iceberg_required)
        return session

    monkeypatch.setattr(SparkHelper, 'get_spark', start)
    backend = SparkEngine()
    backend.check_config(iceberg_job())
    backend.start()
    assert required == [True]


def test_iceberg_input_requires_runtime():
    config = Config.from_dict({
        'engine': 'spark', 'inputs': [{
            'name': 'data', 'uri': 'local.market.trades',
            'parameters': {'format': 'iceberg'},
        }],
    })
    backend = SparkEngine()
    backend.check_config(config)
    assert backend.iceberg_required
    assert backend.iceberg_catalogs == {'local'}


def test_existing_session_without_catalog_fails_before_extraction():
    session = MagicMock()
    session.conf.get.return_value = None
    pipeline = Pipeline(iceberg_job(), spark=session)
    with pytest.raises(RuntimeError, match="Iceberg catalog 'local'.*Restart"):
        pipeline.run()
    session.read.assert_not_called()
    assert pipeline.metrics['stages'] == []


def test_shared_session_accounts_for_later_iceberg_job(monkeypatch):
    calls = []

    def run(pipeline):
        calls.append((pipeline.config.name, pipeline.engine.iceberg_required))
        pipeline.metrics = {'duration_s': 0, 'status': 'success'}

    monkeypatch.setattr(Pipeline, 'run', run)
    later = iceberg_job()
    later.order = 2
    wrapper = WrapperPipeline(GroupConfig(configs=[
        Config(name='first', engine='spark', order=1), later,
    ]))
    wrapper.launch()
    assert calls == [('first', True), ('iceberg', True)]
    assert not wrapper.errors
