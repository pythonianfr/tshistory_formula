import pandas as pd
import pytest


def tuples2series(series_as_tuples, index_name=None, name='indicator'):
    """Convert a list of (index, value) to a pandas Series"""
    idx, values = zip(*series_as_tuples)
    series = pd.Series(
        values,
        index=idx,
        name=name,
    )
    if index_name:
        series.index.name = index_name
    return series


def test_cache_resample_hourly2daily(tsa):
    from datetime import datetime
    series_name = 'constant-values-hourly'
    for day in [1, 2, 3, 4, 5, 6]:
        series = tuples2series(
            [
                (datetime(2020, 1, day, hour), 1)
                for hour in range(24)
            ],
            name=series_name,
        )
        tsa.update(
            series_name,
            series,
            'test_cache_resample',
            insertion_date=datetime(2020, 1, day, 12, 35)
        )
    formula_name = 'constant-values-hourly-resampled-daily'
    tsa.register_formula(
        formula_name,
        f'(resample (series "{series_name}") "D" "sum")',
    )
    pd.testing.assert_series_equal(
        tsa.get(formula_name, from_value_date=datetime(2020, 1, 4, 12)),
        tuples2series(
            [
                (datetime(2020, 1, 4), 24.),
                (datetime(2020, 1, 5), 24.),
                (datetime(2020, 1, 6), 24.),
            ],
            name=formula_name,
        ),
        check_freq=False,
    )
    pd.testing.assert_series_equal(
        tsa.get(formula_name, to_value_date=datetime(2020, 1, 3, 12)),
        tuples2series(
            [
                (datetime(2020, 1, 1), 24.),
                (datetime(2020, 1, 2), 24.),
                (datetime(2020, 1, 3), 24.),
            ],
            name=formula_name,
        ),
        check_freq=False,
    )
    pd.testing.assert_series_equal(
        tsa.get(formula_name, from_value_date=datetime(2020, 1, 3), to_value_date=datetime(2020, 1, 5)),
        tuples2series(
            [
                (datetime(2020, 1, 3), 24.),
                (datetime(2020, 1, 4), 24.),
            ],
            name=formula_name,
        ),
        check_freq=False,
    )
