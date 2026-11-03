"""Common operations executed with the selected engine's native API."""
import operator

COMPARISONS = {
    'eq': operator.eq, 'ne': operator.ne, 'gt': operator.gt,
    'ge': operator.ge, 'lt': operator.lt, 'le': operator.le,
}


def apply_operations(df, operations, engine):
    for spec in operations:
        op = spec['op']
        cols = spec.get('columns', [])
        if op == 'select':
            df = df.loc[:, cols].copy() if engine == 'pandas' else df.select(cols)
        elif op == 'drop':
            df = df.drop(columns=cols) if engine == 'pandas' else df.drop(*cols)
        elif op == 'rename':
            mapping = spec['mapping']
            if engine == 'pandas':
                df = df.rename(columns=mapping)
            elif engine == 'polars':
                df = df.rename(mapping)
            else:
                for old, new in mapping.items():
                    df = df.withColumnRenamed(old, new)
        elif op == 'filter':
            compare = COMPARISONS[spec['operator']]
            if engine == 'polars':
                import polars as pl
                condition = compare(pl.col(spec['column']), spec['value'])
            else:
                condition = compare(df[spec['column']], spec['value'])
            df = (df.loc[condition.fillna(False)].copy()
                  if engine == 'pandas' else df.filter(condition))
        elif op == 'fill_null':
            values = spec['values']
            if engine == 'pandas':
                df = df.fillna(values)
            elif engine == 'polars':
                import polars as pl
                df = df.with_columns([
                    pl.col(c).fill_null(v) for c, v in values.items()
                ])
            else:
                df = df.na.fill(values)
        elif op == 'sort':
            if engine == 'pandas':
                df = df.sort_values(cols, na_position='last')
            elif engine == 'polars':
                df = df.sort(cols, nulls_last=True)
            else:
                from pyspark.sql import functions as F
                df = df.orderBy(*[F.col(c).asc_nulls_last() for c in cols])
        elif op == 'unique':
            if engine == 'pandas':
                df = df.drop_duplicates(subset=cols or None)
            elif engine == 'polars':
                df = df.unique(subset=cols or None, maintain_order=True)
            else:
                df = df.dropDuplicates(cols or None)
        elif op == 'group_by':
            aggs = spec['aggregations']
            if not set(aggs.values()) <= {'sum', 'mean', 'min', 'max', 'count'}:
                raise ValueError('Unsupported aggregation')
            if engine == 'pandas':
                df = df.groupby(cols, dropna=False, as_index=False).agg(aggs)
            elif engine == 'polars':
                import polars as pl
                df = df.group_by(cols).agg([
                    getattr(pl.col(c), fn)().alias(c) for c, fn in aggs.items()
                ])
            else:
                from pyspark.sql import functions as F
                df = df.groupBy(*cols).agg(*[
                    getattr(F, fn)(c).alias(c) for c, fn in aggs.items()
                ])
        else:
            raise ValueError(f'Unsupported DataFrame operation: {op}')
    return df
