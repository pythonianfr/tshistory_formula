from datetime import datetime
import pandas as pd

from test.tools import tuples2series


def test_resample_hourly2daily(tsa):
    series_name = 'constant-values-hourly'
    series = tuples2series(
        [
            (datetime(2020, 1, day, hour), 1)
            for day in [1, 2, 3, 4, 5, 6] for hour in range(24)
        ],
        name=series_name,
    )
    tsa.update(
        series_name,
        series,
        'test_cache_resample',
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
                (datetime(2020, 1, 5), 24.),
            ],
            name=formula_name,
        ),
        check_freq=False,
    )
