import inspect
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from tshistory.util import empty_series
from tshistory.testutil import assert_df

from tshistory_formula import funcs


TZ_BERLIN = 'Europe/Berlin'
USER = 'pytest-test-doy'


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


def linear_series(days, month=1, year=2020):
    """Create a linear series on given month (default is jan. 2020)"""
    return pd.Series(
        days,
        index=[
            pd.Timestamp(f"{year}-{month}-{day}")
            for day in days
        ]
    )


def generate_name(kind):
    """Return name for series/formula within test"""
    name = f'{kind}_for_' + inspect.stack()[1][0].f_code.co_name
    print(f'Name generated: "{name}"')
    return name


def test_empty_series():
    assert funcs.doy_aggregation(empty_series(False), 1).empty


def test_3y_on_3y_formula(tsa):
    series_name = generate_name('series')
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 202000 + 8),
            (datetime(2020, 8, 30), 202000 + 30),
            (datetime(2020, 12, 10), 202000 + 10),
            (datetime(2021, 1, 8), 202100 + 8),
            (datetime(2021, 8, 30), 202100 + 30),
            (datetime(2021, 12, 10), 202100 + 10),
            (datetime(2022, 1, 8), 202200 + 8),
            (datetime(2022, 8, 30), 202200 + 30),
            (datetime(2022, 12, 10), 202200 + 10),
        ],
        name=series_name,
    )
    tsa.update(series_name, series, USER)

    formula_name = generate_name('formula')
    tsa.register_formula(
        formula_name,
        f'(doy-agg (series "{series_name}") 3)',
    )

    ts = tsa.get(formula_name)
    pd.testing.assert_series_equal(
        ts,
        tuples2series(
            [
                (datetime(2023, 1, 8), 202108.),
                (datetime(2023, 8, 30), 202130.),
                (datetime(2023, 12, 10), 202110.),
            ],
            index_name='datetime',
            name=formula_name,
        ),
    )


def test_3y_on_3y_tz(tsa):
    series = tuples2series(
        [
            (pd.Timestamp('2020-1-8', tz=TZ_BERLIN), 202000 + 8),
            (pd.Timestamp('2020-8-30', tz=TZ_BERLIN), 202000 + 30),
            (pd.Timestamp('2020-12-10', tz=TZ_BERLIN), 202000 + 10),
            (pd.Timestamp('2021-1-8', tz=TZ_BERLIN), 202100 + 8),
            (pd.Timestamp('2021-8-30', tz=TZ_BERLIN), 202100 + 30),
            (pd.Timestamp('2021-12-10', tz=TZ_BERLIN), 202100 + 10),
            (pd.Timestamp('2022-1-8', tz=TZ_BERLIN), 202200 + 8),
            (pd.Timestamp('2022-8-30', tz=TZ_BERLIN), 202200 + 30),
            (pd.Timestamp('2022-12-10', tz=TZ_BERLIN), 202200 + 10),
        ],
    )
    pd.testing.assert_series_equal(
        funcs.doy_aggregation(series, 3),
        tuples2series(
            [
                (pd.Timestamp('2023-1-8'), 202108.),
                (pd.Timestamp('2023-8-30'), 202130.),
                (pd.Timestamp('2023-12-10'), 202110.),
            ],
            index_name='datetime',
        ),
    )


def test_3y_depth_on_3y_50prc_ratio():
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 202000 + 8),
            (datetime(2020, 8, 30), np.nan),
            (datetime(2020, 12, 10), 202000 + 10),
            (datetime(2021, 1, 8), np.nan),
            (datetime(2021, 12, 10), 202100 + 10),
            (datetime(2022, 1, 8), 202200 + 8),
            (datetime(2022, 8, 30), 202200 + 30),
            (datetime(2022, 12, 10), 202200 + 10),
        ]
    )
    pd.testing.assert_series_equal(
        funcs.doy_aggregation(series, depth=3, valid_aggr_ratio=0.5),
        tuples2series(
            [
                (datetime(2023, 1, 8), (202008.0 + 202208.) / 2),
                (datetime(2023, 12, 10), 202110.),
            ],
            index_name='datetime',
        ),
    )


def test_2y_depth_on_3y():
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 202000 + 8),
            (datetime(2020, 8, 30), 202000 + 30),
            (datetime(2020, 12, 10), 202000 + 10),
            (datetime(2021, 1, 8), 202100 + 8),
            (datetime(2021, 8, 30), 202100 + 30),
            (datetime(2021, 12, 10), 202100 + 10),
            (datetime(2022, 1, 8), 202200 + 8),
            (datetime(2022, 8, 30), 202200 + 30),
            (datetime(2022, 12, 10), 202200 + 10),
        ]
    )
    pd.testing.assert_series_equal(
        funcs.doy_aggregation(series, depth=2),
        tuples2series(
            [
                (datetime(2022, 1, 8), (202008.0 + 202108.) / 2),
                (datetime(2022, 8, 30), (202030.0 + 202130.) / 2),
                (datetime(2022, 12, 10), (202010.0 + 202110.) / 2),
                (datetime(2023, 1, 8), (202108.0 + 202208.) / 2),
                (datetime(2023, 8, 30), (202130.0 + 202230.) / 2),
                (datetime(2023, 12, 10), (202110.0 + 202210.) / 2),
            ],
            index_name='datetime',
        ),
    )


def test_3y_on_2y_formula_get_with_boundaries(tsa):
    series_name = generate_name('series')
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 202000 + 8),
            (datetime(2020, 8, 30), 202000 + 30),
            (datetime(2020, 12, 10), 202000 + 10),
            (datetime(2021, 1, 8), 202100 + 8),
            (datetime(2021, 8, 30), 202100 + 30),
            (datetime(2021, 12, 10), 202100 + 10),
            (datetime(2022, 1, 8), 202200 + 8),
            (datetime(2022, 8, 30), 202200 + 30),
            (datetime(2022, 12, 10), 202200 + 10),
        ],
        name=series_name,
    )
    tsa.update(series_name, series, USER)

    formula_name = generate_name('formula')
    tsa.register_formula(
        formula_name,
        f'(doy-agg (series "{series_name}") 2)',
    )

    ts = tsa.get(
        formula_name,
        from_value_date=pd.Timestamp('2022-01-08'),
        to_value_date=pd.Timestamp('2022-12-10'),
    )
    pd.testing.assert_series_equal(
        ts,
        tuples2series(
            [
                (datetime(2022, 1, 8), (202008.0 + 202108.) / 2),
                (datetime(2022, 8, 30), (202030.0 + 202130.) / 2),
                (datetime(2022, 12, 10), (202010.0 + 202110.) / 2),
            ],
            index_name='datetime',
            name=formula_name,
        ),
    )


def test_3y_on_2y_formula_with_slice(tsa):
    series_name = generate_name('series')
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 202000 + 8),
            (datetime(2020, 8, 30), 202000 + 30),
            (datetime(2020, 12, 10), 202000 + 10),
            (datetime(2021, 1, 8), 202100 + 8),
            (datetime(2021, 8, 30), 202100 + 30),
            (datetime(2021, 12, 10), 202100 + 10),
            (datetime(2022, 1, 8), 202200 + 8),
            (datetime(2022, 8, 30), 202200 + 30),
            (datetime(2022, 12, 10), 202200 + 10),
        ],
        name=series_name,
    )
    tsa.update(series_name, series, USER)

    subformula_name = generate_name('subformula')
    tsa.register_formula(
        subformula_name,
        f'(doy-agg (series "{series_name}") 2)',
    )
    formula_name = generate_name('formula')
    tsa.register_formula(
        formula_name,
        (
            f'('
            f'  slice'
            f'  (series "{subformula_name}")'
            f'  #:fromdate (date "2022-01-08")'
            f'  #:todate (date "2022-12-10")'
            f')'
        ),
    )

    ts = tsa.get(formula_name)
    pd.testing.assert_series_equal(
        ts,
        tuples2series(
            [
                (datetime(2022, 1, 8), (202008.0 + 202108.) / 2),
                (datetime(2022, 8, 30), (202030.0 + 202130.) / 2),
                (datetime(2022, 12, 10), (202010.0 + 202110.) / 2),
            ],
            index_name='datetime',
            name=formula_name,
        ),
    )


def test_leapday_not_leapyear():
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 1.),
            (datetime(2020, 2, 29), 2.),
            (datetime(2020, 12, 10), 3.),
        ]
    )
    pd.testing.assert_series_equal(
        funcs.doy_aggregation(series, depth=1),
        tuples2series(
            [
                (datetime(2021, 1, 8), 1.),
                (datetime(2021, 12, 10), 3.),
            ],
            index_name='datetime',
        ),
    )


def test_leapday_linear():
    series = tuples2series(
        [
            (pd.Timestamp('2019-2-28', tz=TZ_BERLIN), 1.),
            (pd.Timestamp('2019-3-2', tz=TZ_BERLIN), 4.),
        ]
    )
    pd.testing.assert_series_equal(
        funcs.doy_aggregation(series, depth=1),
        tuples2series(
            [
                (datetime(2020, 2, 28), 1.),
                (datetime(2020, 2, 29), 2.),
                (datetime(2020, 3, 2), 4.),
            ],
            index_name='datetime',
        ),
    )


def test_tz_leapday_linear_formula(tsa):
    # Broken because:
    #  tz aware series is converted to UTC when saved in db
    #  In the case of TZ_BERLIN it shifts the index by -2 hours, that shifts the dates by -1
    series_name = generate_name('series')
    series = tuples2series(
        [
            (pd.Timestamp('2019-2-28', tz=TZ_BERLIN), 1.),
            (pd.Timestamp('2019-3-2', tz=TZ_BERLIN), 4.),
        ]
    )
    tsa.update(series_name, series, USER)

    formula_name = generate_name('formula')
    tsa.register_formula(
        formula_name,
        f'(doy-agg (series "{series_name}") 1)'
    )

    ts = tsa.get(formula_name)
    assert_df("""
datetime
2020-02-27    1.0
2020-02-29    3.0
2020-03-01    4.0
""", ts)


def test_leapday_as_is_missing():
    series = tuples2series(
        [
            (datetime(2019, 2, 28), 1.),
            (datetime(2019, 3, 2), 4.),
        ]
    )
    pd.testing.assert_series_equal(
        funcs.doy_aggregation(series, depth=1, leap_day_rule='as_is'),
        tuples2series(
            [
                (datetime(2020, 2, 28), 1.),
                (datetime(2020, 3, 2), 4.),
            ],
            index_name='datetime',
        ),
    )


def test_leapday_as_is():
    series = tuples2series(
        [
            (datetime(2016, 2, 28), 1.),
            (datetime(2016, 2, 29), 2.),
            (datetime(2016, 3, 1), 3.),
            (datetime(2016, 3, 2), 4.),
            (datetime(2017, 2, 28), 1.),
            (datetime(2017, 3, 2), 4.),
            (datetime(2018, 2, 28), 1.),
            (datetime(2018, 3, 2), 4.),
            (datetime(2019, 2, 28), 1.),
            (datetime(2019, 3, 2), 4.),
        ]
    )
    pd.testing.assert_series_equal(
        funcs.doy_aggregation(
            series, depth=4, leap_day_rule='as_is', valid_aggr_ratio=0.0
        ),
        tuples2series(
            [
                (datetime(2020, 2, 28), 1.),
                (datetime(2020, 2, 29), 2.),
                (datetime(2020, 3, 1), 3.),
                (datetime(2020, 3, 2), 4.),
            ],
            index_name='datetime',
        ),
    )


def test_leapday_as_is_nan():
    series = tuples2series(
        [
            (datetime(2016, 2, 28), 1.),
            (datetime(2016, 2, 29), np.nan),
            (datetime(2016, 3, 1), 3.),
            (datetime(2016, 3, 2), 4.),
            (datetime(2017, 2, 28), 1.),
            (datetime(2017, 3, 2), 4.),
            (datetime(2018, 2, 28), 1.),
            (datetime(2018, 3, 2), 4.),
            (datetime(2019, 2, 28), 1.),
            (datetime(2019, 3, 2), 4.),
        ]
    )
    pd.testing.assert_series_equal(
        funcs.doy_aggregation(
            series, depth=4, leap_day_rule='as_is', valid_aggr_ratio=0.0
        ),
        tuples2series(
            [
                (datetime(2020, 2, 28), 1.),
                (datetime(2020, 3, 1), 3.),
                (datetime(2020, 3, 2), 4.),
            ],
            index_name='datetime',
        ),
    )


def test_leapday_ignore_formula(tsa):
    series_name = generate_name('series')
    series = tuples2series(
        [
            (datetime(2016, 2, 28), 1.),
            (datetime(2016, 2, 29), 2.),
            (datetime(2016, 3, 1), 3.),
            (datetime(2016, 3, 2), 4.),
            (datetime(2017, 2, 28), 1.),
            (datetime(2017, 3, 2), 4.),
            (datetime(2018, 2, 28), 1.),
            (datetime(2018, 3, 2), 4.),
            (datetime(2019, 2, 28), 1.),
            (datetime(2019, 3, 2), 4.),
        ]
    )
    tsa.update(series_name, series, USER)

    formula_name = generate_name('formula')
    tsa.register_formula(
        formula_name,
        f'(doy-agg (series "{series_name}")'
        f'         4'
        f'         #:leap_day_rule "ignore"'
        f'         #:valid_aggr_ratio 0.'
        f')',
    )

    ts = tsa.get(formula_name)
    pd.testing.assert_series_equal(
        ts,
        tuples2series(
            [
                (datetime(2020, 2, 28), 1.),
                (datetime(2020, 3, 1), 3.),
                (datetime(2020, 3, 2), 4.),
            ],
            index_name='datetime',
            name=formula_name,
        ),
    )


def test_2y_depth_on_1y():
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 202000 + 8),
            (datetime(2020, 8, 30), 202000 + 30),
            (datetime(2020, 12, 10), 202000 + 10),
        ]
    )
    assert funcs.doy_aggregation(series, depth=2).empty


# test helpers for operator

@pytest.fixture
def series_3y() -> pd.Series:
    index = [
        pd.Timestamp(2020, 1, 8),
        pd.Timestamp(2020, 8, 30),
        pd.Timestamp(2021, 1, 8),
        pd.Timestamp(2021, 8, 30),
        pd.Timestamp(2022, 1, 8),
        pd.Timestamp(2022, 8, 30),
    ]
    return pd.Series(list(range(len(index))), index=index)


def test_get_boundaries(series_3y):
    assert funcs.get_boundaries(series_3y, 1) == (
        pd.Timestamp(2021, 1, 8), pd.Timestamp(2023, 8, 30)
    )
    assert funcs.get_boundaries(series_3y, 3) == (
        pd.Timestamp(2023, 1, 8), pd.Timestamp(2023, 8, 30)
    )


def test_get_boundaries_too_small(series_3y):
    with pytest.raises(ValueError):
        funcs.get_boundaries(series_3y, 4)


def test_get_boundaries_one_date():
    index = [
        pd.Timestamp(2020, 1, 8),
        pd.Timestamp(2020, 8, 30),
        pd.Timestamp(2021, 1, 8),
    ]
    series = pd.Series(list(range(len(index))), index=index)
    assert funcs.get_boundaries(series, 2) == (
        pd.Timestamp(2022, 1, 8),
        pd.Timestamp(2022, 1, 8)
    )


def test_get_boundaries_type(series_3y):
    res = funcs.get_boundaries(series_3y, 1)
    assert isinstance(res[0], pd.Timestamp)
    assert isinstance(res[1], pd.Timestamp)


def test_get_boundaries_tz(series_3y):
    series_3y.index = series_3y.index.tz_localize(TZ_BERLIN)
    assert funcs.get_boundaries(series_3y, 1) == (
        pd.Timestamp('2021-1-8'),
        pd.Timestamp('2023-8-30'),
    )
    assert funcs.get_boundaries(series_3y, 3) == (
        pd.Timestamp('2023-1-8'),
        pd.Timestamp('2023-8-30'),
    )


def test_get_boundaries_not_round_day(series_3y):
    series_3y.index = series_3y.index + timedelta(hours=8)
    assert funcs.get_boundaries(series_3y, 3) == (
        pd.Timestamp(2023, 1, 8), pd.Timestamp(2023, 8, 30)
    )


def test_aggregation_1y_no_hole():
    series = tuples2series([
        (pd.Timestamp('2020-1-8', tz=TZ_BERLIN), 202000 + 8.),
        (pd.Timestamp('2020-8-30', tz=TZ_BERLIN), 202000 + 30.),
        (pd.Timestamp('2020-12-10', tz=TZ_BERLIN), 202000 + 10.),
    ])
    pd.testing.assert_frame_equal(
        funcs.aggregate_by_doy(
            series,
            from_year=2020,
            to_year=2020,
        ),
        pd.DataFrame([
            {'day_of_year': '01-08', 'indicator': 202008., 'values_count': 1},
            {'day_of_year': '08-30', 'indicator': 202030., 'values_count': 1},
            {'day_of_year': '12-10', 'indicator': 202010., 'values_count': 1},
        ]).set_index('day_of_year', drop=True)
    )


def test_aggregation_1y_no_hole_tz():
    series = tuples2series([
        (datetime(2020, 1, 8), 202000 + 8.),
        (datetime(2020, 8, 30), 202000 + 30.),
        (datetime(2020, 12, 10), 202000 + 10.),
    ])
    pd.testing.assert_frame_equal(
        funcs.aggregate_by_doy(
            series,
            from_year=2020,
            to_year=2020,
        ),
        pd.DataFrame([
            {'day_of_year': '01-08', 'indicator': 202008., 'values_count': 1},
            {'day_of_year': '08-30', 'indicator': 202030., 'values_count': 1},
            {'day_of_year': '12-10', 'indicator': 202010., 'values_count': 1},
        ]).set_index('day_of_year', drop=True)
    )


def test_aggregation_1y_with_holes():
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 202000 + 8.),
            (datetime(2021, 8, 30), np.nan),
            (datetime(2021, 12, 10), 202000 + 10.),
        ]
    )
    pd.testing.assert_frame_equal(
        funcs.aggregate_by_doy(
            series,
            from_year=2021,
            to_year=2021,
        ),
        pd.DataFrame(
            [
                {'day_of_year': '08-30', 'indicator': np.nan, 'values_count': 0},
                {'day_of_year': '12-10', 'indicator': 202010., 'values_count': 1},
            ]
        ).set_index('day_of_year', drop=True)
    )


def test_aggregation_3y_with_holes():
    series = tuples2series([
        (datetime(2020, 1, 8), 202000 + 8),
        (datetime(2020, 8, 30), 202000 + 30),
        (datetime(2020, 12, 10), 202000 + 10),
        (datetime(2021, 8, 30), np.nan),
        (datetime(2021, 12, 10), 202100 + 10),
        (datetime(2022, 1, 8), np.nan),
        (datetime(2022, 8, 30), 202200 + 30),
        (datetime(2022, 12, 10), 202200 + 10),
    ])
    pd.testing.assert_frame_equal(
        funcs.aggregate_by_doy(
            series,
            from_year=2020,
            to_year=2022,
        ),
        pd.DataFrame(
            [
                {'day_of_year': '01-08', 'indicator': 202008., 'values_count': 1},
                {'day_of_year': '08-30', 'indicator': 202130., 'values_count': 2},
                {'day_of_year': '12-10', 'indicator': 202110., 'values_count': 3},
            ]
        ).set_index('day_of_year', drop=True)
    )


def test_aggregation_3y_with_holes_sum():
    series = tuples2series(
        [
            (datetime(2020, 1, 8), 202000 + 8),
            (datetime(2020, 8, 30), 202000 + 30),
            (datetime(2020, 12, 10), 202000 + 10),
            (datetime(2021, 8, 30), np.nan),
            (datetime(2021, 12, 10), 202100 + 10),
            (datetime(2022, 1, 8), np.nan),
            (datetime(2022, 8, 30), 202200 + 30),
            (datetime(2022, 12, 10), 202200 + 10),
        ]
    )
    pd.testing.assert_frame_equal(
        funcs.aggregate_by_doy(
            series,
            from_year=2020,
            to_year=2022,
            method='sum',
        ),
        pd.DataFrame(
            [
                {'day_of_year': '01-08', 'indicator': 202008, 'values_count': 1},
                {'day_of_year': '08-30', 'indicator': 202130.*2, 'values_count': 2},
                {'day_of_year': '12-10', 'indicator': 202110.*3, 'values_count': 3},
            ]
        ).set_index('day_of_year', drop=True)
    )


# test linear_insert_date

def test_linear_insert_date_basic():
    series = linear_series([1, 3, 6])
    funcs.linear_insert_date(series, pd.Timestamp('2020-1-2'))
    pd.testing.assert_series_equal(
        series,
        linear_series([1, 2, 3, 6]),
        check_dtype=False,
    )
    funcs.linear_insert_date(series, pd.Timestamp('2020-1-5'))
    pd.testing.assert_series_equal(
        series,
        linear_series([1, 2, 3, 5, 6]),
        check_dtype=False,
    )


def test_linear_insert_date_not_linear_series():
    series = pd.Series(
        [1000, 2, 4, 5000],
        index=[
            pd.Timestamp('2020-1-1'),
            pd.Timestamp('2020-1-2'),
            pd.Timestamp('2020-1-4'),
            pd.Timestamp('2020-1-5'),
        ]
    )
    pd.testing.assert_series_equal(
        funcs.linear_insert_date(
            series.copy(),
            pd.Timestamp('2020-1-3')
        ),
        pd.Series(
            [1000, 2, 3, 4, 5000],
            index=[
                pd.Timestamp('2020-1-1'),
                pd.Timestamp('2020-1-2'),
                pd.Timestamp('2020-1-3'),
                pd.Timestamp('2020-1-4'),
                pd.Timestamp('2020-1-5'),
            ]
        ),
        check_dtype=False,
    )


def test_linear_insert_date_outside_boundaries():
    series = linear_series([1, 3])
    funcs.linear_insert_date(
        series,
        pd.Timestamp('2020-1-5')
    )
    pd.testing.assert_series_equal(
        series,
        linear_series([1, 3]),
        check_dtype=False,
    )


def test_linear_insert_date_existing():
    series = pd.Series(
        [1, 4, 5],
        index=[
            pd.Timestamp('2020-1-1'),
            pd.Timestamp('2020-1-3'),
            pd.Timestamp('2020-1-5')
        ]
    )
    pd.testing.assert_series_equal(
        funcs.linear_insert_date(
            series.copy(),
            pd.Timestamp('2020-1-3')
        ),
        series,
        check_dtype=False,
    )


def test_linear_insert_date_tz():
    series = pd.Series(
        [1, 3],
        index=[
            pd.Timestamp('2020-1-8', tz=TZ_BERLIN),
            pd.Timestamp('2020-8-30', tz=TZ_BERLIN),
        ],
    )
    pd.testing.assert_series_equal(
        funcs.linear_insert_date(
            series,
            pd.Timestamp('2020-05-04 12:30:00+0200', tz=TZ_BERLIN)
        ),
        pd.Series(
            [1, 2, 3],
            index=[
                pd.Timestamp('2020-1-8', tz=TZ_BERLIN),
                pd.Timestamp('2020-05-04 12:30', tz=TZ_BERLIN),
                pd.Timestamp('2020-8-30', tz=TZ_BERLIN),
            ],
        ),
        check_dtype=False,
    )
