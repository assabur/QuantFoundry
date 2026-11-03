"""Generate offline demo data or normalize a day of Binance spot trades."""
import argparse
from datetime import date
import hashlib
from pathlib import Path
import re
from tempfile import TemporaryDirectory
import zipfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src.market.surveillance import COLUMNS

SCHEMA = pa.schema([
    ('symbol', pa.string()), ('trade_id', pa.int64()),
    ('timestamp_us', pa.int64()), ('price', pa.float64()),
    ('quantity', pa.float64()),
])


def demo_trades():
    """Two instruments, 75 minutes, a volume/price anomaly at minute 65."""
    start = 1735689600000000
    rows = []
    for symbol, base in [('BTCUSDT', 100_000.0), ('ETHUSDT', 3_000.0)]:
        for minute in range(75):
            for tick in range(3):
                rows.append((symbol, minute * 3 + tick,
                             start + minute * 60_000_000 + tick * 1_000_000,
                             base * (1.03 if minute >= 65 else 1),
                             5.0 if minute == 65 else 1.0))
    # Exercise deterministic deduplication and invalid-row rejection.
    rows.extend([rows[0], ('BTCUSDT', 9999, start, -1.0, 1.0)])
    return pd.DataFrame(rows, columns=COLUMNS).astype({
        'trade_id': 'int64', 'timestamp_us': 'int64',
    })


def normalize_chunk(chunk, symbol, day):
    result = chunk.rename(columns={0: 'trade_id', 1: 'price',
                                   2: 'quantity', 4: 'timestamp_us'})
    result['symbol'] = symbol
    result['timestamp_us'] = result.timestamp_us.astype('int64')
    if day < date(2025, 1, 1):
        result['timestamp_us'] *= 1000
    return result[COLUMNS]


def download_trades(output, symbols, day):
    """Bounded-memory normalization; verify archive before reading CSV chunks."""
    with TemporaryDirectory() as temporary:
        staged = Path(temporary) / 'trades.parquet'
        with pq.ParquetWriter(staged, SCHEMA) as writer:
            for symbol in symbols:
                if not re.fullmatch('[A-Z0-9]+', symbol):
                    raise ValueError(f'Invalid symbol: {symbol}')
                filename = f'{symbol}-trades-{day.isoformat()}.zip'
                url = ('https://data.binance.vision/data/spot/daily/trades/'
                       f'{symbol}/{filename}')
                checksum = requests.get(url + '.CHECKSUM', timeout=60)
                checksum.raise_for_status()
                expected = checksum.text.split()[0]
                archive = Path(temporary) / filename
                digest = hashlib.sha256()
                with requests.get(url, stream=True, timeout=60) as response:
                    response.raise_for_status()
                    with archive.open('wb') as target:
                        for block in response.iter_content(1024 * 1024):
                            digest.update(block)
                            target.write(block)
                if digest.hexdigest() != expected:
                    raise ValueError(f'Checksum mismatch: {filename}')
                with zipfile.ZipFile(archive) as zipped:
                    csv_name = filename.removesuffix('.zip') + '.csv'
                    with zipped.open(csv_name) as source:
                        for chunk in pd.read_csv(source, header=None,
                                                 usecols=[0, 1, 2, 4],
                                                 chunksize=250_000):
                            normalized = normalize_chunk(chunk, symbol, day)
                            writer.write_table(pa.Table.from_pandas(
                                normalized, schema=SCHEMA, preserve_index=False,
                            ))
                archive.unlink()
        # Keep an existing destination intact if any download fails.
        import shutil
        shutil.copyfile(staged, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=['demo', 'binance'], default='demo')
    parser.add_argument('--date', type=date.fromisoformat, default=date(2025, 1, 1))
    parser.add_argument('--symbols', nargs='+', default=['BTCUSDT', 'ETHUSDT'])
    parser.add_argument('--output', default='datalake/raw/market/trades.parquet')
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if args.mode == 'demo':
        demo_trades().to_parquet(output, index=False)
    else:
        download_trades(output, args.symbols, args.date)
    print(f'Input ready: {output}')


if __name__ == '__main__':
    main()
