from datetime import date
import os
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
import yaml

from src.market.prepare import demo_trades, normalize_chunk
from src.market.surveillance import (
    clean_trades, minute_bars, detect_anomalies, alerts_only,
)
from src.pipeline import Pipeline
from src.skeleton import Config


def run(df, **params):
    return detect_anomalies(minute_bars(clean_trades(df)), params)


def canonical(df):
    if type(df).__module__.startswith('pyspark'):
        # Format with the UTC Spark session, avoiding host timezone conversion.
        rows = df.selectExpr(
            '* EXCEPT (minute_start)',
            'CAST(minute_start AS STRING) AS minute_start',
        ).collect()
        df = pd.DataFrame([row.asDict() for row in rows])
        df['minute_start'] = pd.to_datetime(df.minute_start)
    if isinstance(df, pl.DataFrame):
        df = df.to_pandas()
    return df.sort_values(['symbol', 'minute_start']).reset_index(drop=True)


def assert_equal(expected, actual):
    pd.testing.assert_frame_equal(
        canonical(expected).sort_index(axis=1),
        canonical(actual).sort_index(axis=1),
        check_dtype=False, atol=1e-10, rtol=1e-10,
    )


def test_native_results_and_expected_business_values():
    trades = demo_trades().sample(frac=1, random_state=42)
    expected = run(trades)
    actual = run(pl.from_pandas(trades))
    assert_equal(expected, actual)
    assert len(clean_trades(trades)) == 450
    alerts = alerts_only(expected)
    assert len(alerts) == 2
    assert set(alerts.symbol) == {'BTCUSDT', 'ETHUSDT'}
    assert (alerts.minute_start == pd.Timestamp('2025-01-01 01:05')).all()
    assert (alerts.volume_mean == 3).all()  # Current spike excluded.
    assert (alerts.volume_ratio == 5).all()
    assert (alerts.history_count == 60).all()
    assert alerts.return_1m.to_list() == pytest.approx([0.03, 0.03])
    assert not expected.loc[expected.history_count < 60, 'alert'].any()


def test_gaps_empty_invalid_and_ties():
    df = demo_trades()
    start = df.timestamp_us.min()
    df = df[~df.timestamp_us.between(
        start + 10 * 60_000_000, start + 11 * 60_000_000 - 1,
    )]
    # Conflicting duplicate: earliest timestamp wins; price/quantity break ties.
    extra = df.iloc[[0]].copy()
    extra['price'] = 1.0
    df = pd.concat([df, extra], ignore_index=True)
    df.loc[len(df)] = ['BTCUSDT', 9998, start, float('inf'), 1]
    df.loc[len(df)] = ['BTCUSDT', 9997, start, float('nan'), 1]
    expected = run(df)
    assert_equal(expected, run(pl.from_pandas(df)))
    after_gap = expected[expected.minute_start == pd.Timestamp('2025-01-01 00:11')]
    assert after_gap.return_1m.isna().all()
    assert not expected[expected.minute_start == pd.Timestamp('2025-01-01 01:05')].alert.any()
    for data in [df.iloc[:0], pl.from_pandas(df.iloc[:0])]:
        result = run(data)
        assert len(result) == 0
        assert 'alert' in result.columns


def test_ohlc_vwap_and_trade_id_order():
    trades = pd.DataFrame([
        ['X', 2, 1_000_000, 110., 3.],
        ['X', 1, 1_000_000, 100., 1.],
    ], columns=['symbol', 'trade_id', 'timestamp_us', 'price', 'quantity'])
    bars = minute_bars(clean_trades(trades))
    assert bars.iloc[0][['open', 'high', 'low', 'close', 'volume', 'vwap']].tolist() == [
        100., 110., 100., 110., 4., 107.5,
    ]
    assert_equal(run(trades), run(pl.from_pandas(trades)))


@pytest.mark.parametrize('engine', ['pandas', 'polars'])
def test_config_pipeline_parquet_roundtrip(engine, tmp_path):
    raw, silver, gold = [tmp_path / name for name in ['raw', 'silver', 'gold']]
    (raw / 'market').mkdir(parents=True)
    demo_trades().to_parquet(raw / 'market/trades.parquet', index=False)
    text = Path(f'config/market/{engine}.yaml').read_text()
    for key, value in [('raw', raw), ('silver', silver), ('gold', gold)]:
        text = text.replace('${' + key + '}', str(value))
    pipeline = Pipeline(Config.from_dict(yaml.safe_load(text)))
    pipeline.run()
    assert pipeline.metrics['status'] == 'success'
    assert len(pd.read_parquet(gold / f'market/{engine}/alerts.parquet')) == 2


def test_binance_timestamp_units():
    for day, timestamp in [(date(2024, 12, 31), 1735603200000),
                           (date(2025, 1, 1), 1735689600000000)]:
        chunk = pd.DataFrame({0: [1], 1: [100.], 2: [2.], 4: [timestamp]})
        result = normalize_chunk(chunk, 'BTCUSDT', day)
        assert result.timestamp_us.iloc[0] == timestamp * (1000 if day.year == 2024 else 1)


def test_spark_matches_native(spark):
    trades = demo_trades()
    expected = run(trades)
    actual = run(spark.createDataFrame(trades))
    assert_equal(expected, actual)
    assert len(alerts_only(run(spark.createDataFrame(trades))).collect()) == 2


@pytest.mark.parametrize('params', [
    {'lookback_minutes': 0}, {'min_history_minutes': 61},
    {'volume_multiplier': -1}, {'price_change_threshold': float('nan')},
])
def test_invalid_parameters(params):
    with pytest.raises(ValueError):
        run(demo_trades(), **params)


@pytest.mark.skipif(
    os.getenv('RUN_ICEBERG_TESTS') != '1',
    reason='Requires RUN_ICEBERG_TESTS=1 and Iceberg runtime JAR',
)
def test_market_iceberg_roundtrip(tmp_path, spark):
    spark.conf.set('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog')
    spark.conf.set('spark.sql.catalog.local.type', 'hadoop')
    spark.conf.set('spark.sql.catalog.local.warehouse',
                   (tmp_path / 'warehouse').as_uri())
    raw = tmp_path / 'raw'
    (raw / 'market').mkdir(parents=True)
    trades = demo_trades()
    trades.to_parquet(raw / 'market/trades.parquet', index=False)
    text = Path('config/market/spark.yaml').read_text().replace('${raw}', str(raw))
    config = Config.from_dict(yaml.safe_load(text))
    Pipeline(config, spark=spark).run()
    assert spark.table('local.market.trades').count() == 450
    assert_equal(run(trades), spark.table('local.market.indicators'))
    assert spark.table('local.market.alerts').count() == 2
    # Re-execution replaces data instead of duplicating it.
    Pipeline(config, spark=spark).run()
    assert spark.table('local.market.alerts').count() == 2
    assert spark.table('local.market.alerts.snapshots').count() == 2


def test_download_normalizes_verified_archive_and_preserves_on_failure(tmp_path, monkeypatch):
    import hashlib
    import io
    import zipfile
    from src.market.prepare import download_trades

    payload = io.BytesIO()
    with zipfile.ZipFile(payload, 'w') as archive:
        archive.writestr('BTCUSDT-trades-2024-12-31.csv',
                         '1,100.0,2.0,200.0,1735603200000,true,true\n')
    data = payload.getvalue()

    class Response:
        text = hashlib.sha256(data).hexdigest() + '  archive.zip'

        def raise_for_status(self):
            pass

        def iter_content(self, size):
            yield data

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    monkeypatch.setattr('src.market.prepare.requests.get', lambda *a, **k: Response())
    output = tmp_path / 'trades.parquet'
    download_trades(output, ['BTCUSDT'], date(2024, 12, 31))
    result = pd.read_parquet(output)
    assert result.timestamp_us.tolist() == [1735603200000000]
    assert result.symbol.tolist() == ['BTCUSDT']
    original = output.read_bytes()
    Response.text = 'bad_checksum  archive.zip'
    with pytest.raises(ValueError, match='Checksum mismatch'):
        download_trades(output, ['BTCUSDT'], date(2024, 12, 31))
    assert output.read_bytes() == original
