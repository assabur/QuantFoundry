"""Identical business rules, evaluated with each DataFrame's native engine.

Input: symbol, trade_id, timestamp_us (UTC epoch microseconds), price, quantity.
Prices/quantities are Float64 analytical values, not accounting decimals.
"""
import math

COLUMNS = ['symbol', 'trade_id', 'timestamp_us', 'price', 'quantity']
BAR_COLUMNS = ['symbol', 'minute', 'open', 'high', 'low', 'close',
               'volume', 'notional', 'trade_count', 'vwap']


def _engine(df):
    return type(df).__module__.split('.')[0]


def clean_trades(df, parameters=None):
    """Reject invalid rows; resolve duplicate keys deterministically."""
    order = ['symbol', 'timestamp_us', 'trade_id', 'price', 'quantity']
    engine = _engine(df)
    if engine == 'pandas':
        import pandas as pd
        result = df[COLUMNS].copy()
        for name in COLUMNS[1:]:
            result[name] = pd.to_numeric(result[name], errors='coerce')
        result = result.dropna(subset=COLUMNS)
        mask = result.symbol.str.strip().ne('')
        for name in COLUMNS[1:]:
            mask &= result[name].map(math.isfinite)
        mask &= (result.price > 0) & (result.quantity > 0)
        mask &= (result.trade_id >= 0) & (result.timestamp_us >= 0)
        mask &= (result.trade_id % 1 == 0) & (result.timestamp_us % 1 == 0)
        result = result[mask].astype({'trade_id': 'int64',
                                      'timestamp_us': 'int64',
                                      'price': 'float64', 'quantity': 'float64'})
        return result.sort_values(order).drop_duplicates(['symbol', 'trade_id'])
    if engine == 'polars':
        import polars as pl
        result = df.select(COLUMNS).with_columns(
            *[pl.col(c).cast(pl.Float64, strict=False) for c in COLUMNS[1:]]
        ).drop_nulls()
        valid = pl.col('symbol').str.strip_chars() != ''
        for name in COLUMNS[1:]:
            valid &= pl.col(name).is_finite()
        valid &= (pl.col('price') > 0) & (pl.col('quantity') > 0)
        for name in ['trade_id', 'timestamp_us']:
            valid &= (pl.col(name) >= 0) & (pl.col(name) % 1 == 0)
        return (result.filter(valid).with_columns(
            pl.col('trade_id', 'timestamp_us').cast(pl.Int64)
        ).sort(order).unique(['symbol', 'trade_id'], keep='first',
                             maintain_order=True))
    if engine == 'pyspark':
        from pyspark.sql import functions as F, Window
        result = df.select('symbol', *[
            F.col(c).try_cast('double').alias(c) for c in COLUMNS[1:]
        ]).dropna()
        valid = F.length(F.trim('symbol')) > 0
        for name in COLUMNS[1:]:
            valid &= F.abs(F.col(name)) < float('inf')
        valid &= (F.col('price') > 0) & (F.col('quantity') > 0)
        for name in ['trade_id', 'timestamp_us']:
            valid &= (F.col(name) >= 0) & (F.col(name) % 1 == 0)
        result = result.filter(valid)
        for name in ['trade_id', 'timestamp_us']:
            result = result.withColumn(name, F.col(name).cast('long'))
        window = Window.partitionBy('symbol', 'trade_id').orderBy(
            'timestamp_us', 'price', 'quantity'
        )
        return result.withColumn('_rn', F.row_number().over(window)).filter(
            '_rn = 1'
        ).drop('_rn')
    raise TypeError(f'Unsupported DataFrame engine: {engine}')


def minute_bars(df, parameters=None):
    """UTC half-open minute buckets; execution order = timestamp then trade ID."""
    engine = _engine(df)
    if engine == 'pandas':
        result = df.sort_values(['symbol', 'timestamp_us', 'trade_id']).copy()
        result['minute'] = result.timestamp_us // 60_000_000 * 60
        result['notional'] = result.price * result.quantity
        bars = result.groupby(['symbol', 'minute'], as_index=False).agg(
            open=('price', 'first'), high=('price', 'max'),
            low=('price', 'min'), close=('price', 'last'),
            volume=('quantity', 'sum'), notional=('notional', 'sum'),
            trade_count=('trade_id', 'size'),
        )
        bars['vwap'] = bars.notional / bars.volume
        return bars[BAR_COLUMNS]
    if engine == 'polars':
        import polars as pl
        bars = (df.sort(['symbol', 'timestamp_us', 'trade_id']).with_columns(
            (pl.col('timestamp_us') // 60_000_000 * 60).alias('minute'),
            (pl.col('price') * pl.col('quantity')).alias('notional'),
        ).group_by('symbol', 'minute').agg(
            pl.col('price').first().alias('open'),
            pl.col('price').max().alias('high'),
            pl.col('price').min().alias('low'),
            pl.col('price').last().alias('close'),
            pl.col('quantity').sum().alias('volume'),
            pl.col('notional').sum(), pl.len().cast(pl.Int64).alias('trade_count'),
        ).with_columns((pl.col('notional') / pl.col('volume')).alias('vwap')))
        return bars.select(BAR_COLUMNS).sort('symbol', 'minute')
    if engine == 'pyspark':
        from pyspark.sql import functions as F
        order = F.struct('timestamp_us', 'trade_id')
        bars = df.withColumn('minute',
                             (F.col('timestamp_us') / 60_000_000).cast('long') * 60)
        return bars.groupBy('symbol', 'minute').agg(
            F.min_by('price', order).alias('open'),
            F.max('price').alias('high'), F.min('price').alias('low'),
            F.max_by('price', order).alias('close'),
            F.sum('quantity').alias('volume'),
            F.sum(F.col('price') * F.col('quantity')).alias('notional'),
            F.count('*').alias('trade_count'),
        ).withColumn('vwap', F.col('notional') / F.col('volume')).select(BAR_COLUMNS)
    raise TypeError(f'Unsupported DataFrame engine: {engine}')


def detect_anomalies(df, parameters=None):
    """Previous wall-clock window excludes current minute; gaps are not filled."""
    params = parameters or {}
    minutes = params.get('lookback_minutes', 60)
    minimum = params.get('min_history_minutes', minutes)
    multiple = params.get('volume_multiplier', 3.0)
    threshold = params.get('price_change_threshold', 0.02)
    if (not isinstance(minutes, int) or isinstance(minutes, bool) or minutes < 1
            or not isinstance(minimum, int) or not 1 <= minimum <= minutes
            or not math.isfinite(multiple) or multiple <= 0
            or not math.isfinite(threshold) or threshold < 0):
        raise ValueError('Invalid surveillance thresholds or history window')
    engine = _engine(df)
    if engine == 'pandas':
        import pandas as pd
        parts = []
        for _, group in df.sort_values(['symbol', 'minute']).groupby('symbol'):
            group = group.copy()
            group.index = pd.to_datetime(group.minute, unit='s')
            group['return_1m'] = (group.close / group.close.shift() - 1).where(
                group.minute.diff() == 60
            )
            previous = group.rolling(f'{minutes}min', closed='left', min_periods=1)
            group['history_count'] = previous.volume.count().fillna(0).astype('int64')
            group['volume_mean'] = previous.volume.mean()
            group['volatility'] = previous.return_1m.std(ddof=1)
            parts.append(group.reset_index(drop=True))
        result = pd.concat(parts, ignore_index=True) if parts else df.copy()
        if not parts:
            for col in ['return_1m', 'volume_mean', 'volatility']:
                result[col] = pd.Series(dtype='float64')
            result['history_count'] = pd.Series(dtype='int64')
        result['history_ready'] = result.history_count >= minimum
        result['volume_ratio'] = result.volume / result.volume_mean
        result['alert'] = (result.history_ready & (result.volume_ratio > multiple)
                           & (result.return_1m.abs() > threshold)).fillna(False)
        result['minute_start'] = pd.to_datetime(result.minute, unit='s')
        return result.drop(columns='minute')
    if engine == 'polars':
        import polars as pl
        result = df.sort('symbol', 'minute').with_columns(
            pl.when(pl.col('minute').diff().over('symbol') == 60)
            .then(pl.col('close') / pl.col('close').shift().over('symbol') - 1)
            .alias('return_1m'),
            pl.from_epoch('minute', time_unit='s').alias('minute_start'),
        )
        stats = result.rolling('minute_start', period=f'{minutes}m', closed='left',
                               group_by='symbol').agg(
            pl.len().cast(pl.Int64).alias('history_count'),
            pl.col('volume').mean().alias('volume_mean'),
            pl.col('return_1m').std(ddof=1).alias('volatility'),
        )
        return (result.join(stats, on=['symbol', 'minute_start']).with_columns(
            (pl.col('history_count') >= minimum).alias('history_ready'),
            (pl.col('volume') / pl.col('volume_mean')).alias('volume_ratio'),
        ).with_columns((pl.col('history_ready')
                        & (pl.col('volume_ratio') > multiple)
                        & (pl.col('return_1m').abs() > threshold))
                       .fill_null(False).alias('alert')).drop('minute'))
    if engine == 'pyspark':
        from pyspark.sql import functions as F, Window
        order = Window.partitionBy('symbol').orderBy('minute')
        previous = order.rangeBetween(-minutes * 60, -1)
        result = df.withColumn('return_1m', F.when(
            F.col('minute') - F.lag('minute').over(order) == 60,
            F.col('close') / F.lag('close').over(order) - 1,
        ))
        result = result.withColumns({
            'history_count': F.count('*').over(previous),
            'volume_mean': F.avg('volume').over(previous),
            'volatility': F.stddev_samp('return_1m').over(previous),
            'minute_start': F.timestamp_seconds('minute'),
        }).withColumns({
            'history_ready': F.col('history_count') >= minimum,
            'volume_ratio': F.col('volume') / F.col('volume_mean'),
        })
        return result.withColumn('alert', F.coalesce(
            F.col('history_ready') & (F.col('volume_ratio') > multiple)
            & (F.abs('return_1m') > threshold), F.lit(False),
        )).drop('minute')
    raise TypeError(f'Unsupported DataFrame engine: {engine}')


def alerts_only(df, parameters=None):
    if _engine(df) == 'pandas':
        return df.loc[df.alert].copy()
    if _engine(df) == 'polars':
        import polars as pl
        return df.filter(pl.col('alert'))
    return df.filter('alert = true')
