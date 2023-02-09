from datetime import datetime
import pandas as pd
import pytest

from test.tools import tuples2series
from tshistory_formula.funcs import get_resample_interval_endpoint


@pytest.mark.parametrize("dt,freq,kind,expected", [
    ('2020-01-01 08:37:56', 'D', 'left', '2020-01-01'),
    ('2020-01-01 08:37:56', 'D', 'right', '2020-01-01 23:59:59.999999'),
    ('2020-01-01', 'D', 'left', '2020-01-01'),
    ('2020-01-01', 'D', 'right', '2020-01-01 23:59:59.999999'),
    ('2020-01-01 08:37:56', 'H', 'left', '2020-01-01 08:00'),
    ('2020-01-01 08:37:56', 'H', 'right', '2020-01-01 08:59:59.999999'),
    ('2020-01-01 08:00', 'H', 'left', '2020-01-01 08:00'),
    ('2020-01-01 08:00', 'H', 'right', '2020-01-01 08:59:59.999999'),
    ('2020-01-01 08:37:56', 'min', 'left', '2020-01-01 08:37:00'),
    ('2020-01-01 08:37:56', 'min', 'right', '2020-01-01 08:37:59.999999'),
    ('2020-01-01 08:37:56', '30S', 'left', '2020-01-01 08:37:30'),
    ('2020-01-01 08:37:56', '30S', 'right', '2020-01-01 08:37:59.999999'),
])
def test_get_resample_interval_endpoint(dt, freq, kind, expected):
    assert get_resample_interval_endpoint(pd.Timestamp(dt), freq, kind) == pd.Timestamp(expected)


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
