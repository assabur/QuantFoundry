"""Native implementations of the quality rules used by listing jobs."""
import json
import logging
import os
from pathlib import Path
from uuid import uuid4

RULES = {
    'expect_column_values_to_not_be_null',
    'expect_column_values_to_be_unique',
    'expect_column_values_to_match_regex',
}


def validate_native(engine, config, df):
    params = config.parameters or {}
    rule = params['rule']
    if rule not in RULES:
        raise ValueError(f'Unsupported native quality rule: {rule}')
    columns = params['columns']
    columns = [columns] if isinstance(columns, str) else columns
    events = []
    for column in columns:
        series = df[column]
        if engine == 'pandas':
            if rule.endswith('not_be_null'):
                unexpected = int(series.isna().sum())
            elif rule.endswith('be_unique'):
                unexpected = int(series.dropna().duplicated(keep=False).sum())
            else:
                regex = (params.get('rule_kwargs') or {})['regex']
                values = series.dropna().astype(str)
                unexpected = int((~values.str.contains(regex)).sum())
        else:
            import polars as pl
            if rule.endswith('not_be_null'):
                unexpected = series.null_count()
            elif rule.endswith('be_unique'):
                unexpected = int(series.drop_nulls().is_duplicated().sum())
            else:
                regex = (params.get('rule_kwargs') or {})['regex']
                valid = series.drop_nulls().cast(pl.String).str.contains(regex)
                unexpected = int((~valid).sum())
        events.append({
            'event': 'quality_result', 'engine': engine,
            'dataset': params.get('dataset_name', config.name),
            'column': column, 'rule': rule,
            'unexpected_count': unexpected, 'success': unexpected == 0,
        })
    directory = Path(os.getenv('SILVER', './datalake/silver')).joinpath(
        'ge_logs', params.get('dataset_name', config.name),
        params.get('output_table_name', config.name),
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f'validation-{uuid4()}.ndjson'
    path.write_text(''.join(json.dumps(e) + '\n' for e in events))
    for event in events:
        logging.getLogger('quantfoundry.quality').info(json.dumps(event))
    return events
