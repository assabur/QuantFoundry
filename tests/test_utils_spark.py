from src.utils.spark import SparkHelper

def test_setup_core_config_contains_expected_keys():
    helper = SparkHelper()
    cfg = helper.setup_core_config()
    expected = {"spark.app.name", "spark.master"}
    assert expected.issubset(set(cfg.keys()))


def test_iceberg_is_opt_in(monkeypatch):
    monkeypatch.setenv('ICEBERG_ENABLED', 'false')
    cfg = SparkHelper().build_config()
    assert cfg['spark.jars.packages'] == 'org.postgresql:postgresql:42.7.4'
    assert 'spark.sql.catalog.local' not in cfg


def test_iceberg_catalog_config(monkeypatch, tmp_path):
    warehouse = tmp_path / 'iceberg'
    monkeypatch.setenv('ICEBERG_ENABLED', 'true')
    monkeypatch.setenv('ICEBERG_WAREHOUSE', str(warehouse))
    cfg = SparkHelper().build_config()
    assert SparkHelper.ICEBERG_PACKAGE in cfg['spark.jars.packages']
    assert 'org.postgresql:postgresql:42.7.4' in cfg['spark.jars.packages']
    assert cfg['spark.sql.catalog.local.type'] == 'hadoop'
    assert cfg['spark.sql.catalog.local.warehouse'] == warehouse.as_uri()
    assert cfg['spark.sql.extensions'].endswith('IcebergSparkSessionExtensions')


def test_iceberg_job_enables_runtime_even_without_environment_flag(monkeypatch):
    monkeypatch.setenv('ICEBERG_ENABLED', 'false')
    cfg = SparkHelper(iceberg_required=True).build_config()
    assert SparkHelper.ICEBERG_PACKAGE in cfg['spark.jars.packages']
    assert cfg['spark.sql.catalog.local'] == 'org.apache.iceberg.spark.SparkCatalog'
