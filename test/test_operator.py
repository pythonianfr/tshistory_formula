from datetime import datetime as dt
import math
import pytz
import pytest

import numpy as np
import pandas as pd
from psyl import lisp
from dateutil.relativedelta import relativedelta

from tshistory.testutil import (
    assert_df,
    assert_hist,
    utcdt
)
from tshistory_formula.registry import (
    finder,
    func,
    metadata
)
from tshistory_formula.interpreter import Interpreter
from tshistory_formula.funcs import compute_bounds


def test_naive_tzone(engine, tsh):
    x = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2020, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, x, 'non-naive', 'Babar')

    tsh.register_formula(
        engine,
        'to-naive',
        '(naive (series "non-naive") "Europe/Paris")',
    )
    ts = tsh.get(engine, 'to-naive')
    assert_df("""
2020-01-01 01:00:00    1.0
2020-01-02 01:00:00    2.0
2020-01-03 01:00:00    3.0
""", ts)

    meta = tsh.internal_metadata(engine, 'to-naive')
    assert meta['tzaware'] == False

    tsh.update(
        engine,
        pd.Series(
            range(5),
            pd.date_range(utcdt(2020, 10, 25), periods=5, freq='H')
        ),
        'non-naive',
        'Celeste'
    )
    ts = tsh.get(engine, 'to-naive')
    assert_df("""
2020-01-01 01:00:00    1.0
2020-01-02 01:00:00    2.0
2020-01-03 01:00:00    3.0
2020-10-25 02:00:00    0.5
2020-10-25 03:00:00    2.0
2020-10-25 04:00:00    3.0
2020-10-25 05:00:00    4.0
""", ts)


def test_naive_vs_dst(engine, tsh):
   ts = pd.Series(
       range(10),
       index=pd.date_range(
           utcdt(2020, 10, 24, 22),
           periods=10,
           freq='H'
       )
   )
   tsh.update(
       engine,
       ts,
       'naive-dst',
       'Babar'
   )

   tsh.register_formula(
       engine,
       'dst-naive',
       '(naive (series "naive-dst") "CET")',
   )

   ts = tsh.get(engine, 'dst-naive')
   assert_df("""
2020-10-25 00:00:00    0.0
2020-10-25 01:00:00    1.0
2020-10-25 02:00:00    2.5
2020-10-25 03:00:00    4.0
2020-10-25 04:00:00    5.0
2020-10-25 05:00:00    6.0
2020-10-25 06:00:00    7.0
2020-10-25 07:00:00    8.0
2020-10-25 08:00:00    9.0
""", ts)


def test_naive_over_naive(engine, tsh):
    x = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2020, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, x, 'naive-series', 'Babar')

    tsh.register_formula(
        engine,
        'naive-over-naive',
        '(naive (series "naive-series") "Europe/Paris")',
    )
    ts = tsh.get(engine, 'naive-over-naive')
    assert_df("""
2020-01-01    1.0
2020-01-02    2.0
2020-01-03    3.0
Name: naive-series, dtype: float64
""", ts)

    meta = tsh.internal_metadata(engine, 'naive-over-naive')
    assert meta['tzaware'] == False


def test_naive_registration(engine, tsh):
    @func('tzaware-autotrophic')
    def tzauto() -> pd.Series:
        return pd.Series(
            [1., 2., 3., 4., 5.],
            index=pd.date_range(utcdt(2020, 10, 25), periods=5, freq='H')
        )

    @metadata('tzaware-autotrophic')
    def tzauto(cn, tsh, tree):
        return {
            'tzaware-autotrophic' : {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

    @finder('tzaware-autotrophic')
    def tzauto(cn, tsh, tree):
        return {tree[0]: tree}

    tsh.update(
        engine,
        pd.Series(
            [1., 2., 3., 4., 5],
            index=pd.date_range(dt(2020, 10, 25), periods=5, freq='H')
        ),
        'really-naive',
        'Celeste'
    )

    tsh.register_formula(
        engine,
        'combine-naive-non-naive-1',
        '(add (series "really-naive")'
        '     (naive (tzaware-autotrophic) "Europe/Paris"))'
    )

    tsh.register_formula(
        engine,
        'combine-naive-non-naive-2',
        '(add (naive (tzaware-autotrophic) "Europe/Paris")'
        '     (series "really-naive"))'
    )

    # embed a bit deeper
    tsh.register_formula(
        engine,
        'combine-naive-non-naive-3',
        '(add (naive (add '
        '                 (tzaware-autotrophic)'
        '                 (+ 3 (tzaware-autotrophic)))'
        '      "Europe/Paris")'
        '     (series "really-naive"))'
    )

    with pytest.raises(ValueError) as err:
        tsh.register_formula(
            engine,
            'combine-naive-non-naive-4',
            '(add (naive (add '
            '                 (tzaware-autotrophic)'
            '                 (+ 3 (tzaware-autotrophic)))'
            '      "Europe/Paris")'
            '     (tzaware-autotrophic))'
        )
    assert err.value.args[0] == (
        "Formula `tzaware-autotrophic` has tzaware vs tznaive series:"
        "`('tzaware-autotrophic', ('add, 'naive, 'add, 'tzaware-autotrophic)):tznaive`,"
        "`('tzaware-autotrophic', ('add, 'naive, 'add, '+, 'tzaware-autotrophic)):tznaive`,"
        "`('tzaware-autotrophic', ('add, 'tzaware-autotrophic)):tzaware`"
    )


def test_naive_tz_boundaries(engine, tsh):
    ts_hourly = pd.Series(
        range(24 * 3 + 1),
        index=pd.date_range(start=utcdt(2022, 2, 1),
                            end=utcdt(2022, 2, 4),
                            freq='H')
    )
    tsh.update(engine, ts_hourly, 'hourly-utc', 'test')

    # building a series localized in EST ->
    # the offset produces a more clear false ouput
    tsh.register_formula(
        engine,
        'resampled-daily-naive',
        '(resample (naive (series "hourly-utc") "EST") "D")'
    )

    ts = tsh.get(
        engine,
        'resampled-daily-naive',
        from_value_date=dt(2022, 2, 2),
        to_value_date=dt(2022, 2, 3)
    )

    assert_df("""
2022-02-02    40.5
2022-02-03    62.5
""", ts)

    tsh.register_formula(
        engine,
        'naive-boundaries',
        '(naive (series "hourly-utc") "EST")'
    )
    ts = tsh.get(
        engine,
        'naive-boundaries',
        from_value_date=dt(2022, 2, 2),
        to_value_date=dt(2022, 2, 3)
    )

    assert_df("""
2022-02-02 00:00:00    29.0
2022-02-02 01:00:00    30.0
2022-02-02 02:00:00    31.0
2022-02-02 03:00:00    32.0
2022-02-02 04:00:00    33.0
2022-02-02 05:00:00    34.0
2022-02-02 06:00:00    35.0
2022-02-02 07:00:00    36.0
2022-02-02 08:00:00    37.0
2022-02-02 09:00:00    38.0
2022-02-02 10:00:00    39.0
2022-02-02 11:00:00    40.0
2022-02-02 12:00:00    41.0
2022-02-02 13:00:00    42.0
2022-02-02 14:00:00    43.0
2022-02-02 15:00:00    44.0
2022-02-02 16:00:00    45.0
2022-02-02 17:00:00    46.0
2022-02-02 18:00:00    47.0
2022-02-02 19:00:00    48.0
2022-02-02 20:00:00    49.0
2022-02-02 21:00:00    50.0
2022-02-02 22:00:00    51.0
2022-02-02 23:00:00    52.0
2022-02-03 00:00:00    53.0
""", ts)

    ts = tsh.get(
        engine,
        'naive-boundaries',
        from_value_date=pd.Timestamp('2022-2-2', tz='EST'),
        to_value_date=pd.Timestamp('2022-2-3', tz='EST')
    )
    assert_df("""
2022-02-02 00:00:00    29.0
2022-02-02 01:00:00    30.0
2022-02-02 02:00:00    31.0
2022-02-02 03:00:00    32.0
2022-02-02 04:00:00    33.0
2022-02-02 05:00:00    34.0
2022-02-02 06:00:00    35.0
2022-02-02 07:00:00    36.0
2022-02-02 08:00:00    37.0
2022-02-02 09:00:00    38.0
2022-02-02 10:00:00    39.0
2022-02-02 11:00:00    40.0
2022-02-02 12:00:00    41.0
2022-02-02 13:00:00    42.0
2022-02-02 14:00:00    43.0
2022-02-02 15:00:00    44.0
2022-02-02 16:00:00    45.0
2022-02-02 17:00:00    46.0
2022-02-02 18:00:00    47.0
2022-02-02 19:00:00    48.0
2022-02-02 20:00:00    49.0
2022-02-02 21:00:00    50.0
2022-02-02 22:00:00    51.0
2022-02-02 23:00:00    52.0
2022-02-03 00:00:00    53.0
""", ts)

    exp = tsh.expanded_formula(
        engine, 'naive-boundaries',
        from_value_date=pd.Timestamp('2022-2-2', tz='EST')
    )
    assert exp == (
        '(let revision_date nil'
        ' from_value_date (date "2022-02-02T00:00:00-05:00" "EST")'
        ' to_value_date nil'
        ' (let from_value_date (tzaware-stamp from_value_date "EST")'
        ' to_value_date (tzaware-stamp to_value_date "EST")'
        ' (naive (series "hourly-utc") "EST")))'
    )

    exp = tsh.expanded_formula(
        engine, 'naive-boundaries',
        from_value_date=pd.Timestamp('2022-2-2')
    )
    assert exp == (
        '(let revision_date nil'
        ' from_value_date (date "2022-02-02T00:00:00" nil)'
        ' to_value_date nil'
        ' (let from_value_date (tzaware-stamp from_value_date "EST")'
        ' to_value_date (tzaware-stamp to_value_date "EST")'
        ' (naive (series "hourly-utc") "EST")))'
    )


def test_add(engine, tsh):
    tsh.register_formula(
        engine,
        'addseries',
        '(add '
        '  (series "x" #:fill "ffill")'
        '  (series "y" #:fill "bfill")'
        '  (series "z" #:fill 0))',
        False
    )

    idate = utcdt(2019, 1, 1)
    x = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )

    tsh.update(engine, x, 'x', 'Babar',
               insertion_date=idate)

    y = pd.Series(
        [7, 8, 9],
        index=pd.date_range(dt(2019, 1, 3), periods=3, freq='D')
    )

    tsh.update(engine, y, 'y', 'Babar',
               insertion_date=idate)

    z = pd.Series(
        [0],
        index=pd.date_range(dt(2019, 1, 3), periods=1, freq='D')
    )

    tsh.update(engine, z, 'z', 'Babar',
               insertion_date=idate)

    ts = tsh.get(engine, 'addseries')
    assert_df("""
2019-01-01     8.0
2019-01-02     9.0
2019-01-03    10.0
2019-01-04    11.0
2019-01-05    12.0
""", ts)

    limited = tsh.get(
        engine, 'addseries',
        from_value_date=dt(2019, 1, 3),
        to_value_date=dt(2019, 1, 4)
    )
    assert_df("""
2019-01-03    10.0
2019-01-04    11.0
""", limited)

    # make some history
    idate2 = utcdt(2019, 1, 2)
    x = pd.Series(
        [2, 3, 4],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )

    tsh.update(engine, x, 'x', 'Babar',
               insertion_date=idate2)

    y = pd.Series(
        [8, 9, 10],
        index=pd.date_range(dt(2019, 1, 3), periods=3, freq='D')
    )

    tsh.update(engine, y, 'y', 'Babar',
               insertion_date=idate2)

    ts = tsh.get(engine, 'addseries')
    assert_df("""
2019-01-01    10.0
2019-01-02    11.0
2019-01-03    12.0
2019-01-04    13.0
2019-01-05    14.0
""", ts)

    ts = tsh.get(engine, 'addseries', revision_date=idate)
    assert_df("""
2019-01-01     8.0
2019-01-02     9.0
2019-01-03    10.0
2019-01-04    11.0
2019-01-05    12.0
""", ts)


def test_serieslist(engine, tsh):
    a = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2023, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, a, 'find.me.a', 'Babar')
    tsh.update(engine, a + 1, 'find.me.a', 'Celeste')

    tsh.register_formula(
        engine,
        'found.them',
        '(add (serieslist (findnames (by.name "find.me"))))'
    )

    ts = tsh.get(engine, 'found.them')
    assert_df("""
2023-01-01    2.0
2023-01-02    3.0
2023-01-03    4.0
""", ts)

    tsh.update(engine, a, 'find.me.b', 'Babar')

    ts = tsh.get(engine, 'found.them')
    assert_df("""
2023-01-01    3.0
2023-01-02    5.0
2023-01-03    7.0
""", ts)

    tsh.register_formula(
        engine,
        'found.none',
        '(add (serieslist (findnames (by.name "no.luck"))))'
    )
    ts = tsh.get(engine, 'found.none')
    assert len(ts) == 0

    tsh.register_formula(
        engine,
        'combine.add-with-empty',
        '(add (series "found.them") (series "found.none"))'
    )
    ts = tsh.get(engine, 'combine.add-with-empty')
    assert len(ts) == 0


def test_more_filter(engine, tsh):
    a = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2023, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, a, 'find.me.A', 'Babar')
    tsh.update(engine, a * 2, 'find.me.B', 'Celeste')
    tsh.update_metadata(
        engine,
        'find.me.A',
        {
            'weight': 42,
            'type': 'A',
            'something': 'something'
         }
    )
    tsh.update_metadata(
        engine,
        'find.me.B',
        {
            'weight': 43,
            'type': 'B'
         }
    )

    tsh.register_formula(
        engine,
        'find.some',
        '(add (serieslist (findnames (by.metakey "something"))))'
    )
    ts = tsh.get(engine, 'find.some')
    assert_df("""
2023-01-01    1.0
2023-01-02    2.0
2023-01-03    3.0
""", ts)

    tsh.register_formula(
        engine,
        'find.some',
        '(add (serieslist (findnames (by.metaitem "type" "A"))))'
    )
    ts = tsh.get(engine, 'find.some')
    assert_df("""
2023-01-01    1.0
2023-01-02    2.0
2023-01-03    3.0
""", ts)

    tsh.register_formula(
        engine,
        'find.some',
        '(add (serieslist (findnames (by.metaitem "weight" 43))))'
    )
    ts = tsh.get(engine, 'find.some')
    assert_df("""
2023-01-01    2.0
2023-01-02    4.0
2023-01-03    6.0
""", ts)

    tsh.register_formula(
        engine,
        'find.some',
        '(add (serieslist (findnames (by.value "weight" "<" 43))))'
    )
    ts = tsh.get(engine, 'find.some')
    assert_df("""
2023-01-01    1.0
2023-01-02    2.0
2023-01-03    3.0
""", ts)

    tsh.register_formula(
        engine,
        'find.some',
        '(add (serieslist (findnames (by.value "weight" "<=" 43))))'
    )
    ts = tsh.get(engine, 'find.some')
    assert_df("""
2023-01-01    3.0
2023-01-02    6.0
2023-01-03    9.0
""", ts)

    tsh.register_formula(
        engine,
        'find.some',
        '(add (serieslist (findnames (by.value "weight" ">" 42))))'
    )
    ts = tsh.get(engine, 'find.some')
    assert_df("""
2023-01-01    2.0
2023-01-02    4.0
2023-01-03    6.0
""", ts)

    tsh.register_formula(
        engine,
        'find.some',
        '(add (serieslist (findnames (by.value "weight" ">=" 42))))'
    )
    ts = tsh.get(engine, 'find.some')
    assert_df("""
2023-01-01    3.0
2023-01-02    6.0
2023-01-03    9.0
""", ts)

    # check findseries
    tsh.register_formula(
        engine,
        'find.some',
        '(add (findseries (by.value "weight" ">=" 42)))'
    )
    ts = tsh.get(engine, 'find.some')
    assert_df("""
2023-01-01    3.0
2023-01-02    6.0
2023-01-03    9.0
""", ts)

    # logical combinators
    tsh.register_formula(
        engine,
        'find.or',
        '(add (findseries '
        '       (by.or '
        '          (by.value "weight" ">" 42)'
        '          (by.name "A"))))'
    )
    ts = tsh.get(engine, 'find.or')
    assert_df("""
2023-01-01    3.0
2023-01-02    6.0
2023-01-03    9.0
""", ts)

    tsh.register_formula(
        engine,
        'find.and',
        '(add (findseries '
        '       (by.and '
        '          (by.value "weight" ">" 42)'
        '          (by.name "B"))))'
    )
    ts = tsh.get(engine, 'find.and')
    assert_df("""
2023-01-01    2.0
2023-01-02    4.0
2023-01-03    6.0
""", ts)

    tsh.register_formula(
        engine,
        'find.not',
        '(add (findseries '
        '       (by.not (by.value "weight" "<" 42))))'
    )
    ts = tsh.get(engine, 'find.and')
    assert_df("""
2023-01-01    2.0
2023-01-02    4.0
2023-01-03    6.0
""", ts)




def test_scalar_div(engine, tsh):
    a = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, a, 'div-me', 'Babar')
    ts = tsh.eval_formula(
        engine,
        '(/ (series "div-me") (/ 3 2))'
    )
    assert_df("""
2019-01-01    0.666667
2019-01-02    1.333333
2019-01-03    2.000000
""", ts)


def test_priority(engine, tsh):
    a = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    b = pd.Series(
        [10, 20, 30],
        index=pd.date_range(dt(2019, 1, 2), periods=3, freq='D')
    )
    c = pd.Series(
        [100, 200, 300],
        index=pd.date_range(dt(2019, 1, 3), periods=3, freq='D')
    )

    tsh.update(engine, a, 'a', 'Babar', insertion_date=utcdt(2020, 1, 1))
    tsh.update(engine, b, 'b', 'Celeste', insertion_date=utcdt(2019, 12, 1))
    tsh.update(engine, c, 'c', 'Arthur', insertion_date=utcdt(2020, 2, 1))

    tsh.register_formula(
        engine,
        'test_prio',
        '(priority (series "c") (series "b") (series "a"))'
    )

    prio = tsh.get(engine, 'test_prio')

    assert_df("""
2019-01-01      1.0
2019-01-02     10.0
2019-01-03    100.0
2019-01-04    200.0
2019-01-05    300.0
""", prio)

    prio = tsh.get(engine, 'test_prio', revision_date=utcdt(2019, 12, 1))
    assert_df("""
2019-01-02    10.0
2019-01-03    20.0
2019-01-04    30.0
""", prio)

    prio = tsh.get(engine, 'test_prio', revision_date=utcdt(2019, 11, 1))
    assert len(prio) == 0

    limited = tsh.get(
        engine,
        'test_prio',
        from_value_date=dt(2019, 1, 2),
        to_value_date=dt(2019, 1, 3)
    )
    assert_df("""
2019-01-02     10.0
2019-01-03    100.0
""", limited)

    # type
    assert tsh.type(engine, 'no-such-series') == 'primary'
    assert tsh.type(engine, 'test_prio') == 'formula'
    assert tsh.type(engine, 'a') == 'primary'
    assert not tsh.exists(engine, 'no-such-series')
    assert tsh.type(engine, 'test_prio')
    assert tsh.type(engine, 'a')

    h = tsh.history(engine, 'test_prio')
    assert_hist("""
insertion_date             value_date
2019-12-01 00:00:00+00:00  2019-01-02     10.0
                           2019-01-03     20.0
                           2019-01-04     30.0
2020-01-01 00:00:00+00:00  2019-01-01      1.0
                           2019-01-02     10.0
                           2019-01-03     20.0
                           2019-01-04     30.0
2020-02-01 00:00:00+00:00  2019-01-01      1.0
                           2019-01-02     10.0
                           2019-01-03    100.0
                           2019-01-04    200.0
                           2019-01-05    300.0
""", h)


def test_priority2(engine, tsh):
    tsh.register_formula(
        engine,
        'test_prio2',
        '(priority (series "real") (series "nom") (series "fcst"))',
        False
    )

    real = pd.Series(
        [1, 1, 1],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    nom = pd.Series(
        [10, 10, 10, 10],
        index=pd.date_range(dt(2019, 1, 1), periods=4, freq='D')
    )
    fcst = pd.Series(
        [100, 100, 100, 100, 100],
        index=pd.date_range(dt(2019, 1, 1), periods=5, freq='D')
    )

    tsh.update(engine, real, 'real', 'Babar')
    tsh.update(engine, nom, 'nom', 'Celeste')
    tsh.update(engine, fcst, 'fcst', 'Arthur')

    prio = tsh.get(engine, 'test_prio2')

    assert_df("""
2019-01-01      1.0
2019-01-02      1.0
2019-01-03      1.0
2019-01-04     10.0
2019-01-05    100.0
""", prio)


def test_priority_one_series(engine, tsh):
    tsh.register_formula(
        engine,
        'test_prio_one',
        '(priority (series "just-a"))',
        False
    )

    a = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, a, 'just-a', 'Babar')

    a = tsh.get(engine, 'test_prio_one')
    assert_df("""
2019-01-01    1.0
2019-01-02    2.0
2019-01-03    3.0
""", a)


def test_clip(engine, tsh):
    tsh.register_formula(
        engine,
        'test_clip',
        '(clip (series "a") #:min 2 #:max 4)'
    )

    a = pd.Series(
        [1, 2, 3, 4, 5],
        index=pd.date_range(dt(2019, 1, 1), periods=5, freq='D')
    )
    tsh.update(engine, a, 'a', 'Babar')

    cleaned = tsh.get(engine, 'test_clip')
    assert_df("""
2019-01-02    2.0
2019-01-03    3.0
2019-01-04    4.0
""", cleaned)

    restricted = tsh.get(
        engine,
        'test_clip',
        from_value_date=dt(2019, 1, 3),
        to_value_date=dt(2019, 1, 3)
    )
    assert_df("""
2019-01-03    3.0
""", restricted)

    tsh.register_formula(
        engine,
        'test_clip_replaced',
        '(clip (series "a") #:min 2 #:max 4 #:replacemin #t #:replacemax #t)'
    )

    replaced = tsh.get(engine, 'test_clip_replaced')
    assert_df("""
2019-01-01    2.0
2019-01-02    2.0
2019-01-03    3.0
2019-01-04    4.0
2019-01-05    4.0
""", replaced)


def test_slice(engine, tsh):
    base = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'test-slice', 'Babar')
    tsh.register_formula(
        engine,
        'slicing-id',
        '(slice (series "test-slice"))',
    )
    tsh.register_formula(
        engine,
        'slicing-from',
        '(slice (series "test-slice") #:fromdate (date "2019-1-2"))',
    )
    tsh.register_formula(
        engine,
        'slicing-fromto',
        '(slice (series "test-slice") '
        ' #:fromdate (date "2019-1-2") '
        ' #:todate (date "2019-1-2")'
        ')',
    )
    tsh.register_formula(
        engine,
        'slicing-empty',
        '(slice (series "test-slice") '
        ' #:fromdate (date "2018-1-2") '
        ' #:todate (date "2018-1-2")'
        ')',
    )

    assert_df("""
2019-01-01 00:00:00+00:00    1.0
2019-01-02 00:00:00+00:00    2.0
2019-01-03 00:00:00+00:00    3.0
""", tsh.get(engine, 'slicing-id'))

    assert_df("""
2019-01-02 00:00:00+00:00    2.0
2019-01-03 00:00:00+00:00    3.0
""", tsh.get(engine, 'slicing-from'))

    assert_df("""
2019-01-02 00:00:00+00:00    2.0
""", tsh.get(engine, 'slicing-fromto'))

    assert len(tsh.get(engine, 'slicing-empty')) == 0


def test_asof_history(engine, tsh):
    for i in range(3):
        base = pd.Series(
            [i] * 5,
            index=pd.date_range(utcdt(2022, 1, 1), periods=5, freq='D')
        )
        tsh.update(
            engine,
            base,
            'test-asof-hist',
            'Babar',
            insertion_date=utcdt(2022, 1, 1 + i)
        )

    tsh.register_formula(
        engine,
        'asof-history',
        '(asof (shifted (today) #:days -1) (series "test-asof-hist"))'
    )

    v0 = tsh.get(engine, 'asof-history', revision_date=utcdt(2022, 1, 1))
    assert not len(v0)

    v1 = tsh.get(engine, 'asof-history', revision_date=utcdt(2022, 1, 2))
    assert_df("""
2022-01-01 00:00:00+00:00    0.0
2022-01-02 00:00:00+00:00    0.0
2022-01-03 00:00:00+00:00    0.0
2022-01-04 00:00:00+00:00    0.0
2022-01-05 00:00:00+00:00    0.0
""", v1)

    v3 = tsh.get(engine, 'asof-history', revision_date=utcdt(2022, 1, 4))
    assert_df("""
2022-01-01 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    2.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    2.0
""", v3)

    hist = tsh.history(
        engine,
        'asof-history'
    )
    assert_hist("""
insertion_date             value_date               
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    0.0
                           2022-01-02 00:00:00+00:00    0.0
                           2022-01-03 00:00:00+00:00    0.0
                           2022-01-04 00:00:00+00:00    0.0
                           2022-01-05 00:00:00+00:00    0.0
2022-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    1.0
""", hist)


def test_slice_naiveseries(engine, tsh):
    base = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2020, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'test-slice-naive', 'Babar')

    tsh.register_formula(
        engine,
        'slicing-naive',
        '(slice (series "test-slice-naive") '
        '       #:fromdate (date "2012-1-2")'
        '       #:todate (date "2020-1-2")'
        ')',
    )

    ts = tsh.get(engine, 'slicing-naive',
                from_value_date=dt(2021, 1, 1))
    assert not len(ts)

    tsh.get(engine, 'slicing-naive')
    # pandas didn't crash us \o/


def test_slice_options(engine, tsh):
    base = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'test-slice', 'Babar')
    # options transmissions
    ts = tsh.eval_formula(
        engine,
        '(add (series "test-slice") '
        '     (slice (series "test-slice" #:fill 0) #:fromdate (date "2019-1-2")))',
    )
    assert_df("""
2019-01-01 00:00:00+00:00    1.0
2019-01-02 00:00:00+00:00    4.0
2019-01-03 00:00:00+00:00    6.0
""", ts)


def test_scalar_pow(engine, tsh):
    base = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'pow-a', 'Babar')

    tsh.register_formula(
        engine,
        'pow-2',
        '(** (series "pow-a") 2)',
    )

    tsh.register_formula(
        engine,
        'pow-sqrt',
        '(** (series "pow-a") 0.5)',
    )

    ts = tsh.get(engine, 'pow-2')
    assert_df("""
2019-01-01 00:00:00+00:00    1.0
2019-01-02 00:00:00+00:00    4.0
2019-01-03 00:00:00+00:00    9.0
""", ts)

    ts = tsh.get(engine, 'pow-sqrt')
    assert_df("""
2019-01-01 00:00:00+00:00    1.000000
2019-01-02 00:00:00+00:00    1.414214
2019-01-03 00:00:00+00:00    1.732051
""", ts)

    ts = tsh.get(engine, 'pow-2',
                 from_value_date=utcdt(2020, 1, 1))
    assert ts.index.tz.zone == 'UTC'


def test_mul(engine, tsh):
    base = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'mul-a', 'Babar')
    tsh.update(engine, base, 'mul-b', 'Babar')
    tsh.update(engine, base, 'mul-c', 'Babar')
    tsh.register_formula(
        engine,
        'multiply-aligned',
        '(mul (series "mul-a") (series "mul-b") (series "mul-c"))',
    )

    ts = tsh.get(engine, 'multiply-aligned')
    assert_df("""
2019-01-01 00:00:00+00:00     1.0
2019-01-02 00:00:00+00:00     8.0
2019-01-03 00:00:00+00:00    27.0
""", ts)

    ts = tsh.get(engine, 'multiply-aligned',
                 from_value_date=utcdt(2020, 1, 1))
    assert ts.index.tz.zone == 'UTC'

    base = pd.Series(
        [1, 2, np.nan],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'mul-b', 'Babar')

    ts = tsh.get(engine, 'multiply-aligned')
    assert_df("""
2019-01-01 00:00:00+00:00    1.0
2019-01-02 00:00:00+00:00    8.0
""", ts)

    tsh.register_formula(
        engine,
        'multiply-aligned',
        '(mul (series "mul-a") (series "mul-b" #:fill 1) (series "mul-c"))'
    )
    ts = tsh.get(engine, 'multiply-aligned')
    assert_df("""
2019-01-01 00:00:00+00:00    1.0
2019-01-02 00:00:00+00:00    8.0
2019-01-03 00:00:00+00:00    9.0
""", ts)


def test_div(engine, tsh):
    base = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'div-a', 'Babar')
    tsh.update(engine, base, 'div-b', 'Babar')
    tsh.register_formula(
        engine,
        'divide',
        '(div (series "div-a") (series "div-b"))',
    )

    ts = tsh.get(engine, 'divide')
    assert_df("""
2019-01-01 00:00:00+00:00    1.0
2019-01-02 00:00:00+00:00    1.0
2019-01-03 00:00:00+00:00    1.0
""", ts)

    base = pd.Series(
        [2, 1, np.nan],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'div-b', 'Babar')

    ts = tsh.get(engine, 'divide')
    assert_df("""
2019-01-01 00:00:00+00:00    0.5
2019-01-02 00:00:00+00:00    2.0
""", ts)

    tsh.register_formula(
        engine,
        'divide',
        '(div (series "div-a") (series "div-b" #:fill 3))'
    )
    ts = tsh.get(engine, 'divide')
    assert_df("""
2019-01-01 00:00:00+00:00    0.5
2019-01-02 00:00:00+00:00    2.0
2019-01-03 00:00:00+00:00    1.0
""", ts)


def _prepare_row_ops(engine, tsh):
    if tsh.exists(engine, 'station0'):
        return

    dates = pd.date_range(
        start=utcdt(2015, 1, 1),
        freq='D',
        periods=8
    )

    station0 = pd.Series([0] * 8, index=dates)
    station1 = pd.Series([1] * 8, index=dates)
    station2 = pd.Series([2] * 8, index=dates)

    # we add some perturbations:
    station0 = station0.drop(station0.index[4])
    station1 = station1.drop(station1.index[2])
    station2 = station2.drop(station2.index[4])
    # line full of nans
    station0 = station0.drop(station0.index[5])
    station1 = station1.drop(station1.index[5])
    station2 = station2.drop(station2.index[5])

    summary = pd.concat(
        [station0, station1, station2],
        axis=1
    )

    assert_df("""
                             0    1    2
2015-01-01 00:00:00+00:00  0.0  1.0  2.0
2015-01-02 00:00:00+00:00  0.0  1.0  2.0
2015-01-03 00:00:00+00:00  0.0  NaN  2.0
2015-01-04 00:00:00+00:00  0.0  1.0  2.0
2015-01-05 00:00:00+00:00  NaN  1.0  NaN
2015-01-06 00:00:00+00:00  0.0  1.0  2.0
2015-01-08 00:00:00+00:00  0.0  1.0  2.0
""", summary)

    tsh.update(engine, station0, 'station0', 'Babar')
    tsh.update(engine, station1, 'station1', 'Celeste')
    tsh.update(engine, station2, 'station2', 'Arthur')


def test_row_mean(engine, tsh):
    _prepare_row_ops(engine, tsh)

    formula = (
        '(row-mean '
        '  (series "station0") '
        '  (series "station1") '
        '  (series "station2" #:weight 2))'
    )

    tsh.register_formula(
        engine,
        'weather_index',
        formula
    )

    avg_index = tsh.get(engine, 'weather_index')
    assert_df("""
2015-01-01 00:00:00+00:00    1.250000
2015-01-02 00:00:00+00:00    1.250000
2015-01-03 00:00:00+00:00    1.333333
2015-01-04 00:00:00+00:00    1.250000
2015-01-05 00:00:00+00:00    1.000000
2015-01-06 00:00:00+00:00    1.250000
2015-01-08 00:00:00+00:00    1.250000
""", avg_index)

    formula = (
        '(row-mean '
        '  (series "station0") '
        '  (series "station1") '
        '  (series "station2" #:weight 2)'
        '  #:skipna #f)'
    )

    tsh.register_formula(
        engine,
        'weather_index_skipna',
        formula
    )
    ts = tsh.get(
        engine, 'weather_index_skipna',
    )
    assert_df("""
2015-01-01 00:00:00+00:00    1.25
2015-01-02 00:00:00+00:00    1.25
2015-01-04 00:00:00+00:00    1.25
2015-01-06 00:00:00+00:00    1.25
2015-01-08 00:00:00+00:00    1.25
""", ts)


def test_min(engine, tsh):
    _prepare_row_ops(engine, tsh)

    formula = '(row-min (series "station0") (series "station1") (series "station2"))'
    tsh.register_formula(
        engine,
        'weather_min',
        formula
    )

    assert_df("""
2015-01-01 00:00:00+00:00    0.0
2015-01-02 00:00:00+00:00    0.0
2015-01-03 00:00:00+00:00    0.0
2015-01-04 00:00:00+00:00    0.0
2015-01-05 00:00:00+00:00    1.0
2015-01-06 00:00:00+00:00    0.0
2015-01-08 00:00:00+00:00    0.0
""", tsh.get(engine, 'weather_min'))

    formula = '(row-min (series "station0") (series "station1") (series "station2") #:skipna #f)'
    tsh.register_formula(
        engine,
        'weather_min_skipna',
        formula
    )
    ts = tsh.get(
        engine, 'weather_min_skipna',
    )
    assert_df("""
2015-01-01 00:00:00+00:00    0.0
2015-01-02 00:00:00+00:00    0.0
2015-01-04 00:00:00+00:00    0.0
2015-01-06 00:00:00+00:00    0.0
2015-01-08 00:00:00+00:00    0.0
""", ts)


def test_max(engine, tsh):
    _prepare_row_ops(engine, tsh)

    formula = '(row-max (series "station0") (series "station1") (series "station2"))'
    tsh.register_formula(
        engine,
        'weather_max',
        formula
    )

    assert_df("""
2015-01-01 00:00:00+00:00    2.0
2015-01-02 00:00:00+00:00    2.0
2015-01-03 00:00:00+00:00    2.0
2015-01-04 00:00:00+00:00    2.0
2015-01-05 00:00:00+00:00    1.0
2015-01-06 00:00:00+00:00    2.0
2015-01-08 00:00:00+00:00    2.0
""", tsh.get(engine, 'weather_max'))

    formula = '(row-max (series "station0") (series "station1") (series "station2") #:skipna #f)'
    tsh.register_formula(
        engine,
        'weather_max_skipna',
        formula
    )
    ts = tsh.get(
        engine, 'weather_max_skipna',
    )
    assert_df("""
2015-01-01 00:00:00+00:00    2.0
2015-01-02 00:00:00+00:00    2.0
2015-01-04 00:00:00+00:00    2.0
2015-01-06 00:00:00+00:00    2.0
2015-01-08 00:00:00+00:00    2.0
""", ts)


def test_std(engine, tsh):
    _prepare_row_ops(engine, tsh)

    formula = '(std (series "station0") (series "station1") (series "station2"))'
    tsh.register_formula(
        engine,
        'weather_std',
        formula
    )

    assert_df("""
2015-01-01 00:00:00+00:00    1.000000
2015-01-02 00:00:00+00:00    1.000000
2015-01-03 00:00:00+00:00    1.414214
2015-01-04 00:00:00+00:00    1.000000
2015-01-06 00:00:00+00:00    1.000000
2015-01-08 00:00:00+00:00    1.000000
""", tsh.get(engine, 'weather_std'))

    formula = '(std (series "station0") (series "station1") (series "station2") #:skipna #f)'
    tsh.register_formula(
        engine,
        'weather_std_skipna',
        formula
    )
    ts = tsh.get(
        engine, 'weather_std_skipna',
    )
    assert_df("""
2015-01-01 00:00:00+00:00    1.0
2015-01-02 00:00:00+00:00    1.0
2015-01-04 00:00:00+00:00    1.0
2015-01-06 00:00:00+00:00    1.0
2015-01-08 00:00:00+00:00    1.0
""", ts)


def test_date(engine, tsh):
    e1 = '(date "2018-1-1")'
    e2 = '(date "2018-1-1 12:00:00" #:tz "Europe/Moscow")'
    e3 = '(date "2020-1-1 06:42:30")'
    e4 = '(date "2020-1-1" #:tz "Gondwana/Chandrapore")'

    i = Interpreter(engine, tsh, {})
    a = lisp.evaluate(e1, i.env)
    b = lisp.evaluate(e2, i.env)
    c = lisp.evaluate(e3, i.env)
    with pytest.raises(pytz.UnknownTimeZoneError) as err:
        lisp.evaluate(e4, i.env)
    assert err.value.args[0] == 'Gondwana/Chandrapore'

    assert a == pd.Timestamp('2018-01-01 00:00:00+0000', tz='UTC')
    assert b == pd.Timestamp('2018-01-01 12:00:00+0300', tz='Europe/Moscow')
    assert c == pd.Timestamp('2020-01-01 06:42:30+0000', tz='UTC')


def test_shifted(engine, tsh):
    e1 = '(shifted (date "2020-1-1"))'  # null
    e2 = '(shifted (date "2020-1-1") #:years 1)'
    e3 = '(shifted (date "2020-1-1") #:months 1) '
    e4 = '(shifted (date "2020-1-1") #:weeks 1)'
    e5 = '(shifted (date "2020-1-1") #:days 1)'
    e6 = '(shifted (date "2020-1-1") #:hours 1)'
    e7 = '(shifted (date "2020-1-1") #:minutes 1)'

    i = Interpreter(engine, tsh, {})
    a = lisp.evaluate(e1, i.env)
    b = lisp.evaluate(e2, i.env)
    c = lisp.evaluate(e3, i.env)
    d = lisp.evaluate(e4, i.env)
    e = lisp.evaluate(e5, i.env)
    f = lisp.evaluate(e6, i.env)
    g = lisp.evaluate(e7, i.env)

    assert a == pd.Timestamp('2020-01-01 00:00:00+0000', tz='UTC')
    assert b == pd.Timestamp('2021-01-01 00:00:00+0000', tz='UTC')
    assert c == pd.Timestamp('2020-02-01 00:00:00+0000', tz='UTC')
    assert d == pd.Timestamp('2020-01-08 00:00:00+0000', tz='UTC')
    assert e == pd.Timestamp('2020-01-02 00:00:00+0000', tz='UTC')
    assert f == pd.Timestamp('2020-01-01 01:00:00+0000', tz='UTC')
    assert g == pd.Timestamp('2020-01-01 00:01:00+0000', tz='UTC')


def test_today(engine, tsh):
    e1 = '(today)'
    e2 = '(today #:naive #t)'
    e3 = '(today #:naive #t #:tz "Europe/Moscow")'
    e4 = '(today #:naive #f)'
    e5 = '(today #:naive #f #:tz "Europe/Moscow")'
    e6 = '(today #:tz "Gondwana/Chandrapore")'

    i = Interpreter(engine, tsh, {})
    a = lisp.evaluate(e1, i.env)
    b = lisp.evaluate(e2, i.env)
    with pytest.raises(AssertionError) as err:
        lisp.evaluate(e3, i.env)
    assert err.value.args[0] == 'date cannot be naive and have a tz'
    d = lisp.evaluate(e4, i.env)
    e = lisp.evaluate(e5, i.env)
    with pytest.raises(pytz.UnknownTimeZoneError) as err:
        lisp.evaluate(e6, i.env)
    assert err.value.args[0] == 'Gondwana/Chandrapore'

    assert a.tz == pytz.utc
    assert b.tz is None
    assert d.tz == pytz.utc
    assert e.tz.zone == 'Europe/Moscow'


def test_more_today(engine, tsh):
    now = pd.Timestamp(dt.now().date(), tz='UTC')
    for d in [-1, 0, 1]:
        ts = pd.Series(
            [d] * 3,
            index=pd.date_range(
                now, periods=3, freq='D'
            )
        )

        tsh.update(
            engine,
            ts,
            'today-base',
            'Babar',
            insertion_date=(now + relativedelta(days=d))
        )

    tsh.register_formula(
        engine,
        'sliced-base',
        '(slice (series "today-base") '
        '       #:fromdate (today)'
        '       #:todate (shifted (today) #:days 10))'
    )

    # last version: as of today + 1 day
    ts_2 = tsh.get(engine, 'sliced-base')
    assert len(ts_2) == 2
    assert ts_2[0] == 1.0
    assert ts_2.index[0] == now + relativedelta(days=1)
    assert ts_2.index[-1] == now + relativedelta(days=2)

    # last version: as of today + 1 day (explicit revision_date)
    # the cutoff is not the same since we look into tomorrow
    # and (today) will be bound to it
    ts_2 = tsh.get(
        engine,
        'sliced-base',
        revision_date=now + relativedelta(days=1)
    )
    assert len(ts_2) == 2
    assert ts_2[0] == 1.0
    assert ts_2.index[0] == now + relativedelta(days=1)
    assert ts_2.index[-1] == now + relativedelta(days=2)

    # first version: as of today - 1 day
    ts_0 = tsh.get(
        engine, 'sliced-base',
        revision_date=now - relativedelta(days=1)
    )
    assert len(ts_0) == 3
    assert ts_0[0] == -1.0
    assert ts_0.index[0] == now

    # middle version: as of today
    ts_1 = tsh.get(
        engine, 'sliced-base',
        revision_date=now
    )
    assert len(ts_1) == 3
    assert ts_1[0] == 0.0
    assert ts_1.index[0] == now

    hist = tsh.history(
        engine,
        'sliced-base'
    )
    assert [3, 3, 2] == list(map(len, hist.values()))
    series = list(hist.values())
    for left, right in zip(series, [ts_0, ts_1, ts_2]):
        assert left.equals(right)


def test_start_of_month(engine, tsh):
    i = Interpreter(engine, tsh, {})
    a = lisp.evaluate('(start-of-month (date "1973-05-20 09:00"))', i.env)
    assert a == pd.Timestamp('1973-05-01 09:00:00+0000', tz='UTC')


def test_end_of_month(engine, tsh):
    i = Interpreter(engine, tsh, {})
    a = lisp.evaluate('(end-of-month (date "1973-05-20 09:00"))', i.env)
    assert a == pd.Timestamp('1973-05-31 09:00:00+0000', tz='UTC')


def test_resample(engine, tsh):
    hourly = pd.Series(
        list(range(36)),
        index=pd.date_range(utcdt(2020, 1, 1), periods=36, freq='H')
    )

    gasday = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2020, 1, 1, 5), periods=3, freq='D')
    )

    tsh.update(engine, hourly, 'hourly', 'Babar')
    tsh.update(engine, gasday, 'gasday', 'Celeste')

    tsh.register_formula(
        engine,
        'hourly2daily',
        '(resample (series "hourly") "D")'
    )
    tsh.register_formula(
        engine,
        'hourly2dailysum',
        '(resample (series "hourly") "D" "sum")'
    )
    tsh.register_formula(
        engine,
        'gasdaytoday',
        '(resample (series "gasday") "D")'
    )
    tsh.register_formula(
        engine,
        'badmethod',
        '(resample (series "gasday") "D" "NO-SUCH-METHOD")'
    )

    assert_df("""
2020-01-01 00:00:00+00:00    11.5
2020-01-02 00:00:00+00:00    29.5
""", tsh.get(engine, 'hourly2daily'))

    assert_df("""
2020-01-01 00:00:00+00:00    276.0
2020-01-02 00:00:00+00:00    354.0
""", tsh.get(engine, 'hourly2dailysum'))

    assert_df("""
2020-01-01 00:00:00+00:00    1.0
2020-01-02 00:00:00+00:00    2.0
2020-01-03 00:00:00+00:00    3.0
""", tsh.get(engine, 'gasdaytoday'))

    with pytest.raises(ValueError) as err:
        tsh.get(engine, 'badmethod')
    assert err.value.args[0] == 'bad resampling method `NO-SUCH-METHOD`'

    gasday['2020-1-2'] = np.nan
    tsh.update(engine, gasday, 'gasday', 'Celeste')
    assert_df("""
2020-01-01 00:00:00+00:00    1.0
2020-01-03 00:00:00+00:00    3.0
""", tsh.get(engine, 'gasdaytoday'))


def test_resample_boundaries(tsh, engine):
    series_name = 'constant-values-hourly'
    for day in (1, 2, 3, 4, 5, 6):
        series = pd.Series(
            [1.] * 24,
            index=pd.date_range(
                dt(2023, 1, day, 0),
                freq='H',
                periods=24
            )
        )
        tsh.update(
            engine,
            series,
            series_name,
            'test_cache_resample',
            insertion_date=pd.Timestamp(f'2023-1-{day} 12:35', tz='utc')
        )
    formula_name = 'constant-values-hourly-resampled-daily'
    tsh.register_formula(
        engine,
        formula_name,
        f'(resample (series "{series_name}") "D" "sum")',
    )

    ts = tsh.get(
        engine,
        formula_name,
        from_value_date=dt(2023, 1, 4, 12)
    )
    ref = pd.Series(
        [24., 24., 24.],
        index=pd.date_range(dt(2023, 1, 4), freq='D', periods=3)
    )

    assert ts.equals(ref)

    ts = tsh.get(
        engine,
        formula_name,
        to_value_date=dt(2023, 1, 3, 12)
    )
    ref = pd.Series(
        [24., 24., 24.],
        index=pd.date_range(dt(2023, 1, 1), freq='D', periods=3)
    )

    assert ts.equals(ref)

    ts = tsh.get(
        engine,
        formula_name,
        from_value_date=dt(2023, 1, 3),
        to_value_date=dt(2023, 1, 5)
    )
    ref = pd.Series(
        [24., 24., 24.],
        index=pd.date_range(dt(2023, 1, 3), freq='D', periods=3)
    )

    assert ts.equals(ref)


@pytest.mark.parametrize("tstamp,freq,direction,expected", [
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
def test_resample_adjusted_stamp(tstamp, freq, direction, expected):
    from tshistory_formula.funcs import resample_adjusted_stamp
    assert resample_adjusted_stamp(
        pd.Timestamp(tstamp),
        freq,
        direction
    ) == pd.Timestamp(expected)


def test_cumsum(engine, tsh):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2020, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        series,
        'sum-me',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'test-cumsum',
        '(cumsum (series "sum-me"))'
    )

    s1 = tsh.get(engine, 'test-cumsum')
    assert_df("""
2020-01-01 00:00:00+00:00    1.0
2020-01-02 00:00:00+00:00    3.0
2020-01-03 00:00:00+00:00    6.0
""", s1)


def test_time_shifted(engine, tsh):
    series = pd.Series(
        [1, 2, 3, 4, 5],
        index=pd.date_range(
            utcdt(2020, 1, 1),
            periods=5,
            freq='D'
        )
    )

    tsh.update(
        engine,
        series,
        'shifted',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'test-shift',
        '(time-shifted (series "shifted") #:days -2)'
    )

    exp = tsh.expanded_formula(
        engine,
        'test-shift'
    )
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (let from_value_date (shifted from_value_date #:days 2)'
        ' to_value_date (shifted to_value_date #:days 2)'
        ' (time-shifted (series "shifted") #:days -2)))'
    )

    s2 = tsh.get(engine, 'test-shift')
    assert_df("""
2019-12-30 00:00:00+00:00    1.0
2019-12-31 00:00:00+00:00    2.0
2020-01-01 00:00:00+00:00    3.0
2020-01-02 00:00:00+00:00    4.0
2020-01-03 00:00:00+00:00    5.0
""", s2)

    s1 = tsh.get(
        engine,
        'test-shift',
        from_value_date=utcdt(2020, 1, 1),
        to_value_date=utcdt(2020, 1, 3)
    )
    assert_df("""
2020-01-01 00:00:00+00:00    3.0
2020-01-02 00:00:00+00:00    4.0
2020-01-03 00:00:00+00:00    5.0
""", s1)

    s1 = tsh.get(
        engine,
        'test-shift',
        to_value_date=utcdt(2020, 1, 1)
    )
    assert_df("""
2019-12-30 00:00:00+00:00    1.0
2019-12-31 00:00:00+00:00    2.0
2020-01-01 00:00:00+00:00    3.0
""", s1)

    tsh.register_formula(
        engine,
        'test-shift-nokw',
        '(time-shifted (series "shifted"))'
    )
    s1 = tsh.get(
        engine,
        'test-shift-nokw'
    )


def test_asof_fixed_date(engine, tsh):
    for v in range(1, 4):
        idate = utcdt(2022, 1, v)
        series = pd.Series(
            [v] * 5,
            index=pd.date_range(
                utcdt(2022, 1, 1),
                periods=5,
                freq='D'
            )
        )

        tsh.update(
            engine,
            series,
            'asof-base-fixed',
            'Babar',
            insertion_date=idate
        )

    tsh.register_formula(
        engine,
        'test-asof-fixed',
        '(asof (date "2022-1-2") (series "asof-base-fixed"))'
    )

    s0 = tsh.get(
        engine,
        'test-asof-fixed'
    )
    assert_df("""
2022-01-01 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    2.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    2.0
""", s0)

    s1 = tsh.get(
        engine,
        'test-asof-fixed',
        revision_date=utcdt(2022, 1, 2)
    )
    # here we see the asof forcing at work
    assert_df("""
2022-01-01 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    2.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    2.0
""", s1)

    s2 = tsh.get(
        engine,
        'test-asof-fixed',
        revision_date=utcdt(2022, 1, 1)
    )
    assert_df("""
2022-01-01 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    2.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    2.0
""", s2)

    h = tsh.history(
        engine,
        'test-asof-fixed'
    )
    assert_hist("""
insertion_date             value_date               
2022-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    2.0
""", h)


def test_asof_fixed2(engine, tsh):
    for v in range(1, 4):
        idate = utcdt(2022, 1, v)
        series = pd.Series(
            [v] * 5,
            index=pd.date_range(
                utcdt(2022, 1, 1),
                periods=5,
                freq='D'
            )
        )

        tsh.update(
            engine,
            series,
            'asof-base-fixed2',
            'Babar',
            insertion_date=idate
        )

    tsh.register_formula(
        engine,
        'test-asof-fixed2',
        '(add (series "asof-base-fixed2")'
        '     (asof (date "2022-1-2") (series "asof-base-fixed2")))'
    )

    s0 = tsh.get(
        engine,
        'test-asof-fixed2'
    )
    assert_df("""
2022-01-01 00:00:00+00:00    5.0
2022-01-02 00:00:00+00:00    5.0
2022-01-03 00:00:00+00:00    5.0
2022-01-04 00:00:00+00:00    5.0
2022-01-05 00:00:00+00:00    5.0
""", s0)

    s1 = tsh.get(
        engine,
        'test-asof-fixed2',
        revision_date=utcdt(2022, 1, 2)
    )
    # here we see the asof forcing at work
    assert_df("""
2022-01-01 00:00:00+00:00    4.0
2022-01-02 00:00:00+00:00    4.0
2022-01-03 00:00:00+00:00    4.0
2022-01-04 00:00:00+00:00    4.0
2022-01-05 00:00:00+00:00    4.0
""", s1)

    s2 = tsh.get(
        engine,
        'test-asof-fixed2',
        revision_date=utcdt(2022, 1, 1)
    )
    assert_df("""
2022-01-01 00:00:00+00:00    3.0
2022-01-02 00:00:00+00:00    3.0
2022-01-03 00:00:00+00:00    3.0
2022-01-04 00:00:00+00:00    3.0
2022-01-05 00:00:00+00:00    3.0
""", s2)

    h = tsh.history(
        engine,
        'test-asof-fixed2'
    )
    assert_hist("""
insertion_date             value_date               
2022-01-01 00:00:00+00:00  2022-01-01 00:00:00+00:00    3.0
                           2022-01-02 00:00:00+00:00    3.0
                           2022-01-03 00:00:00+00:00    3.0
                           2022-01-04 00:00:00+00:00    3.0
                           2022-01-05 00:00:00+00:00    3.0
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    4.0
                           2022-01-02 00:00:00+00:00    4.0
                           2022-01-03 00:00:00+00:00    4.0
                           2022-01-04 00:00:00+00:00    4.0
                           2022-01-05 00:00:00+00:00    4.0
2022-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    5.0
                           2022-01-02 00:00:00+00:00    5.0
                           2022-01-03 00:00:00+00:00    5.0
                           2022-01-04 00:00:00+00:00    5.0
                           2022-01-05 00:00:00+00:00    5.0
""", h)


def test_asof_today(engine, tsh):
    for v in range(1, 4):
        idate = utcdt(2022, 1, v)
        series = pd.Series(
            [v] * 5,
            index=pd.date_range(
                utcdt(2022, 1, 1),
                periods=5,
                freq='D'
            )
        )

        tsh.update(
            engine,
            series,
            'asof-base',
            'Babar',
            insertion_date=idate
        )

    idates = tsh.insertion_dates(
        engine,
        'asof-base'
    )
    assert idates == [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC')
    ]
    idates = tsh.insertion_dates(
        engine,
        'asof-base',
        from_insertion_date=utcdt(2022, 1, 2),
        to_insertion_date=utcdt(2022, 1, 2)
    )
    assert idates == [
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC')
    ]

    tsh.register_formula(
        engine,
        'test-asof-yesterday',
        '(asof (shifted (today) #:days -1) (series "asof-base"))'
    )

    exp = tsh.expanded_formula(
        engine,
        'test-asof-yesterday'
    )
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (let revision_date (shifted (today) #:days -1)'
        ' (asof (shifted (today) #:days -1)'
        ' (series "asof-base"))))'
    )

    s1 = tsh.get(
        engine,
        'test-asof-yesterday',
        revision_date=utcdt(2022, 1, 2)
    )
    assert_df("""
2022-01-01 00:00:00+00:00    1.0
2022-01-02 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00    1.0
2022-01-04 00:00:00+00:00    1.0
2022-01-05 00:00:00+00:00    1.0
""", s1)

    s2 = tsh.get(
        engine,
        'test-asof-yesterday',
        revision_date=utcdt(2022, 1, 1)
    )
    assert not len(s2)

    s3 = tsh.get(
        engine,
        'test-asof-yesterday',
        revision_date=utcdt(2022, 1, 3)
    )
    assert_df("""
2022-01-01 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00    2.0
2022-01-03 00:00:00+00:00    2.0
2022-01-04 00:00:00+00:00    2.0
2022-01-05 00:00:00+00:00    2.0
""", s3)

    h = tsh.history(
        engine,
        'test-asof-yesterday'
    )
    assert_hist("""
insertion_date             value_date               
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    1.0
2022-01-03 00:00:00+00:00  2022-01-01 00:00:00+00:00    2.0
                           2022-01-02 00:00:00+00:00    2.0
                           2022-01-03 00:00:00+00:00    2.0
                           2022-01-04 00:00:00+00:00    2.0
                           2022-01-05 00:00:00+00:00    2.0
""", h)

    h = tsh.history(
        engine,
        'test-asof-yesterday',
        to_insertion_date=utcdt(2022, 1, 2)
    )
    assert_hist("""
insertion_date             value_date               
2022-01-02 00:00:00+00:00  2022-01-01 00:00:00+00:00    1.0
                           2022-01-02 00:00:00+00:00    1.0
                           2022-01-03 00:00:00+00:00    1.0
                           2022-01-04 00:00:00+00:00    1.0
                           2022-01-05 00:00:00+00:00    1.0
""", h)



def test_rolling(engine, tsh):
    series = pd.Series(
        [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5],
        index=pd.date_range(utcdt(2020, 1, 1), periods=15, freq='D')
    )

    tsh.update(
        engine,
        series,
        'rolling',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'test-rolling',
        '(+ 1 (rolling (series "rolling") #:window 2))'
    )

    s1 = tsh.get(engine, 'test-rolling')
    assert_df("""
2020-01-02 00:00:00+00:00    2.0
2020-01-03 00:00:00+00:00    2.0
2020-01-04 00:00:00+00:00    2.5
2020-01-05 00:00:00+00:00    3.0
2020-01-06 00:00:00+00:00    3.0
2020-01-07 00:00:00+00:00    3.5
2020-01-08 00:00:00+00:00    4.0
2020-01-09 00:00:00+00:00    4.0
2020-01-10 00:00:00+00:00    4.5
2020-01-11 00:00:00+00:00    5.0
2020-01-12 00:00:00+00:00    5.0
2020-01-13 00:00:00+00:00    5.5
2020-01-14 00:00:00+00:00    6.0
2020-01-15 00:00:00+00:00    6.0
""", s1)

    tsh.register_formula(
        engine,
        'test-rolling2',
        '(+ 1 (rolling (series "rolling") #:window 2 #:method "sum"))'
    )

    s1 = tsh.get(engine, 'test-rolling2')
    assert_df("""
2020-01-02 00:00:00+00:00     3.0
2020-01-03 00:00:00+00:00     3.0
2020-01-04 00:00:00+00:00     4.0
2020-01-05 00:00:00+00:00     5.0
2020-01-06 00:00:00+00:00     5.0
2020-01-07 00:00:00+00:00     6.0
2020-01-08 00:00:00+00:00     7.0
2020-01-09 00:00:00+00:00     7.0
2020-01-10 00:00:00+00:00     8.0
2020-01-11 00:00:00+00:00     9.0
2020-01-12 00:00:00+00:00     9.0
2020-01-13 00:00:00+00:00    10.0
2020-01-14 00:00:00+00:00    11.0
2020-01-15 00:00:00+00:00    11.0
""", s1)


def test_na_behaviour(engine, tsh):
    series = pd.Series(
        [1, 2, np.nan],
        index=pd.date_range(utcdt(2020, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        series,
        'tsnan',
        'Author'
    )

    # scalar operators
    # *
    ts = tsh.eval_formula(engine, '(* 1 (series "tsnan"))')
    assert_df("""
2020-01-01 00:00:00+00:00    1.0
2020-01-02 00:00:00+00:00    2.0
""", ts)

    # +
    ts = tsh.eval_formula(engine, '(+ 1 (series "tsnan"))')
    assert_df("""
2020-01-01 00:00:00+00:00    2.0
2020-01-02 00:00:00+00:00    3.0
""", ts)

    # /
    ts = tsh.eval_formula(engine, '(/ 1 (series "tsnan"))')
    assert_df("""
2020-01-01 00:00:00+00:00    1.0
2020-01-02 00:00:00+00:00    0.5
""", ts)

    # series to series operator
    series = pd.Series(
        [1] * 3,
        index=pd.date_range(utcdt(2020, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        series,
        'const',
        'Author'
    )

    # add
    ts = tsh.eval_formula(engine, '(add (series "tsnan") (series "const"))')
    assert_df("""
2020-01-01 00:00:00+00:00    2.0
2020-01-02 00:00:00+00:00    3.0
""", ts)

    # mul
    ts = tsh.eval_formula(engine, '(mul (series "tsnan") (series "const"))')
    assert_df("""
2020-01-01 00:00:00+00:00    1.0
2020-01-02 00:00:00+00:00    2.0
""", ts)

    # div
    ts = tsh.eval_formula(engine, '(div (series "tsnan") (series "const"))')
    assert_df("""
2020-01-01 00:00:00+00:00    1.0
2020-01-02 00:00:00+00:00    2.0
""", ts)


def test_out_of_bounds(engine, tsh):
    # short series vs long series
    series = pd.Series(
        [1, 2],
        index=pd.date_range(utcdt(2020, 1, 1), periods=2, freq='D')
    )

    tsh.update(
        engine,
        series,
        'short',
        'Author'
    )

    series = pd.Series(
        [1] * 6,
        index=pd.date_range(utcdt(2020, 1, 1), periods=6, freq='D')
    )

    tsh.update(
        engine,
        series,
        'long',
        'Author'
    )

    formula = '(add (series "short") (series "long"))'
    # add
    tsh.register_formula(engine, 'addition', formula)

    ts = tsh.get(engine, 'addition')
    assert_df("""
2020-01-01 00:00:00+00:00    2.0
2020-01-02 00:00:00+00:00    3.0
""", ts)

    ts = tsh.get(
        engine, 'addition',
        from_value_date=utcdt(2020, 1, 3),
        to_value_date=utcdt(2020, 1, 4),
    )
    # one of the two series is empty on this interval
    # => the sum returns an empty series
    assert not len(ts)

    # with fill parameters
    tsh.register_formula(
        engine,
        'addition_bis',
        '(add (series "short" #:fill -10) (series "long"))'
    )

    ts = tsh.get(engine, 'addition_bis')
    assert_df("""
2020-01-01 00:00:00+00:00    2.0
2020-01-02 00:00:00+00:00    3.0
2020-01-03 00:00:00+00:00   -9.0
2020-01-04 00:00:00+00:00   -9.0
2020-01-05 00:00:00+00:00   -9.0
2020-01-06 00:00:00+00:00   -9.0
""", ts)

    ts = tsh.get(
        engine,
        'addition_bis',
        from_value_date=utcdt(2020, 1, 3),
        to_value_date=utcdt(2020, 1, 4),
    )

    # as expected
    assert_df("""
2020-01-03 00:00:00+00:00   -9.0
2020-01-04 00:00:00+00:00   -9.0
""", ts)


    # with ffill
    tsh.register_formula(
        engine,
        'addition_ter',
        '(add (series "short" #:fill "ffill") (series "long"))'
    )

    ts = tsh.get(engine, 'addition_ter')
    assert_df("""
2020-01-01 00:00:00+00:00    2.0
2020-01-02 00:00:00+00:00    3.0
2020-01-03 00:00:00+00:00    3.0
2020-01-04 00:00:00+00:00    3.0
2020-01-05 00:00:00+00:00    3.0
2020-01-06 00:00:00+00:00    3.0
""", ts)

    ts = tsh.get(
        engine, 'addition_ter',
        from_value_date=utcdt(2020, 1, 3),
        to_value_date=utcdt(2020, 1, 4),
    )
    # with data out of bounds of the short series, ffill is not
    # able to infer the data to fill, hence nothing is returned
    assert not len(ts)


def test_constant(engine, tsh):
    tsh.register_formula(
        engine,
        'constant-1',
        '(constant 1. (date "2020-1-1") (date "2020-1-3") "D" (date "2020-2-1"))'
    )

    ts = tsh.get(engine, 'constant-1')
    assert_df("""
2020-01-01 00:00:00+00:00    1.0
2020-01-02 00:00:00+00:00    1.0
2020-01-03 00:00:00+00:00    1.0
""", ts)

    ts = tsh.get(
        engine,
        'constant-1',
        from_value_date=utcdt(2020, 1, 2),
        to_value_date=utcdt(2020, 1, 2)
    )
    assert_df("""
2020-01-02 00:00:00+00:00    1.0
""", ts)

    ts = tsh.get(
        engine,
        'constant-1',
        from_value_date=utcdt(2020, 1, 1, 6),
        to_value_date=utcdt(2020, 1, 2, 6)
    )

    assert_df("""
2020-01-02 00:00:00+00:00    1.0
""", ts)

    ts = tsh.get(
        engine,
        'constant-1',
        from_value_date=dt(2020, 1, 2),
        to_value_date=dt(2020, 1, 2)
    )
    assert_df("""
2020-01-02 00:00:00+00:00    1.0
""", ts)

    ts = tsh.get(engine, 'constant-1', revision_date=utcdt(2020, 1, 1))
    assert len(ts) == 0
    assert isinstance(ts.index, pd.Index)

    hist = tsh.history(engine, 'constant-1')
    assert_hist("""
insertion_date             value_date               
2020-02-01 00:00:00+00:00  2020-01-01 00:00:00+00:00    1.0
                           2020-01-02 00:00:00+00:00    1.0
                           2020-01-03 00:00:00+00:00    1.0
""", hist)

    hist = tsh.history(engine, 'constant-1', from_insertion_date=utcdt(2020, 2, 2))
    assert len(hist) == 0

    hist = tsh.history(engine, 'constant-1', to_insertion_date=utcdt(2019, 1, 1))
    assert len(hist) == 0

    idates = tsh.insertion_dates(engine, 'constant-1')
    assert idates == [
        pd.Timestamp('2020-02-01 00:00:00+0000', tz='UTC')
    ]

    idates = tsh.insertion_dates(engine, 'constant-1',
                                 from_insertion_date=utcdt(2020, 2, 2))
    assert idates == []
    idates = tsh.insertion_dates(engine, 'constant-1',
                                 to_insertion_date=utcdt(2019, 1, 1))
    assert idates == []

    # edge cases
    tsh.register_formula(
        engine,
        'constant-2',
        '(constant 2. (date "2020-1-5") (date "2020-1-3") "D" (date "2020-2-1"))'
    )

    ts = tsh.get(engine, 'constant-2')
    assert len(ts) == 0

    tsh.register_formula(
        engine,
        'constant-3',
        '(constant 3. (date "2020-1-1") (today) "D" (date "2020-2-1"))'
    )

    # no crash wrt naive revision_date
    tsh.get(engine, 'constant-3', revision_date=dt(2020, 2, 1))
    tsh.get(engine, 'constant-3', from_value_date=dt(2020, 2, 1))
    tsh.get(engine, 'constant-3', to_value_date=dt(2020, 2, 1))


def test_constant_today_timetravel(engine, tsh):
    tsh.register_formula(
        engine,
        'constant-with-today',
        '(constant 1. (date "2021-1-1") (today) "D" (date "2020-2-1"))'
    )

    ts = tsh.get(
        engine,
        'constant-with-today',
        revision_date=pd.Timestamp('2021-1-3', tz='CET')
    )
    assert_df("""
2021-01-01 00:00:00+00:00    1.0
2021-01-02 00:00:00+00:00    1.0
""", ts)


def test_trigo(engine, tsh):
    base = pd.Series(
        [-400, -1, 0, 90, 180, 300000],
        index=pd.date_range(utcdt(2022, 1, 1), periods=6, freq='D')
    )
    tsh.update(engine, base, 'trigo-a', 'Babar')

    tsh.register_formula(
        engine,
        'cosinus',
        '(trig.cos (series "trigo-a"))',
    )

    tsh.register_formula(
        engine,
        'cosinus-round',
        '(trig.cos (series "trigo-a") #:decimals 14)',
    )

    tsh.register_formula(
        engine,
        'sinus',
        '(trig.sin (series "trigo-a"))',
    )

    tsh.register_formula(
        engine,
        'tangent',
        '(trig.tan (series "trigo-a"))',
    )

    ts = tsh.get(engine, 'cosinus')
    assert_df("""
2022-01-01 00:00:00+00:00    7.660444e-01
2022-01-02 00:00:00+00:00    9.998477e-01
2022-01-03 00:00:00+00:00    1.000000e+00
2022-01-04 00:00:00+00:00    6.123234e-17
2022-01-05 00:00:00+00:00   -1.000000e+00
2022-01-06 00:00:00+00:00   -5.000000e-01
""", ts)

    ts = tsh.get(engine, 'cosinus-round')
    assert_df("""
2022-01-01 00:00:00+00:00    0.766044
2022-01-02 00:00:00+00:00    0.999848
2022-01-03 00:00:00+00:00    1.000000
2022-01-04 00:00:00+00:00    0.000000
2022-01-05 00:00:00+00:00   -1.000000
2022-01-06 00:00:00+00:00   -0.500000
""", ts)

    ts = tsh.get(engine, 'sinus')
    assert_df("""
2022-01-01 00:00:00+00:00   -6.427876e-01
2022-01-02 00:00:00+00:00   -1.745241e-02
2022-01-03 00:00:00+00:00    0.000000e+00
2022-01-04 00:00:00+00:00    1.000000e+00
2022-01-05 00:00:00+00:00    1.224647e-16
2022-01-06 00:00:00+00:00    8.660254e-01
""", ts)

    ts = tsh.get(engine, 'tangent')
    assert_df("""
2022-01-01 00:00:00+00:00   -8.390996e-01
2022-01-02 00:00:00+00:00   -1.745506e-02
2022-01-03 00:00:00+00:00    0.000000e+00
2022-01-04 00:00:00+00:00    1.633124e+16
2022-01-05 00:00:00+00:00   -1.224647e-16
2022-01-06 00:00:00+00:00   -1.732051e+00
""", ts)

    ts = tsh.get(engine, 'sinus',
                 from_value_date=utcdt(2023, 1, 1))
    assert ts.index.tz.zone == 'UTC'

    base_coord = pd.Series(
        [-1, -np.sqrt(1/2), 0, 0.76, 1, 90],
        index=pd.date_range(utcdt(2022, 1, 1), periods=6, freq='D')
    )
    tsh.update(engine, base_coord, 'coord-a', 'Babar')

    tsh.register_formula(
        engine,
        'arccosinus',
        '(trig.arccos (series "coord-a"))',
    )

    tsh.register_formula(
        engine,
        'arcsinus',
        '(trig.arcsin (series "coord-a"))',
    )

    tsh.register_formula(
        engine,
        'arctangent',
        '(trig.arctan (series "coord-a"))',
    )

    ts = tsh.get(engine, 'arccosinus')
    assert_df("""
2022-01-01 00:00:00+00:00    180.000000
2022-01-02 00:00:00+00:00    135.000000
2022-01-03 00:00:00+00:00     90.000000
2022-01-04 00:00:00+00:00     40.535802
2022-01-05 00:00:00+00:00      0.000000
""", ts)

    ts = tsh.get(engine, 'arcsinus')
    assert_df("""
2022-01-01 00:00:00+00:00   -90.000000
2022-01-02 00:00:00+00:00   -45.000000
2022-01-03 00:00:00+00:00     0.000000
2022-01-04 00:00:00+00:00    49.464198
2022-01-05 00:00:00+00:00    90.000000
""", ts)

    ts = tsh.get(engine, 'arctangent')
    assert_df("""
2022-01-01 00:00:00+00:00   -45.000000
2022-01-02 00:00:00+00:00   -35.264390
2022-01-03 00:00:00+00:00     0.000000
2022-01-04 00:00:00+00:00    37.234834
2022-01-05 00:00:00+00:00    45.000000
2022-01-06 00:00:00+00:00    89.363406
""", ts)

    ts = tsh.get(engine, 'arcsinus',
                 from_value_date=utcdt(2023, 1, 1))
    assert ts.index.tz.zone == 'UTC'

    tsh.register_formula(
        engine,
        'arctangente2',
        '(trig.row-arctan2 (series "coord-a") (series "coord-a"))',
    )

    ts = tsh.get(engine, 'arctangente2')
    assert_df("""
2022-01-01 00:00:00+00:00   -135.0
2022-01-02 00:00:00+00:00   -135.0
2022-01-03 00:00:00+00:00      0.0
2022-01-04 00:00:00+00:00     45.0
2022-01-05 00:00:00+00:00     45.0
2022-01-06 00:00:00+00:00     45.0
""", ts)

    ts = tsh.get(
        engine,
        'arctangente2',
        from_value_date=pd.Timestamp('2023-1-1'),
        to_value_date=pd.Timestamp('2023-1-1')
    )

    assert not len(ts)


# integration -- big hairy operator
# keep me at the end of this module

def test_base_integration(engine, tsh):
    first_i_date = pd.Timestamp(dt(2015, 1, 1), tz='UTC')
    second_i_date = pd.Timestamp(dt(2015, 1, 2), tz='UTC')

    # first insertion
    ts_stock_obs = pd.Series(
        range(2),
        pd.date_range(
            start=dt(2015, 1, 1),
            end=dt(2015, 1, 2),
            freq='D'
        )
    )

    ts_flow = pd.Series(
        [1, 2] * 5,
        pd.date_range(
            start=dt(2015, 1, 1),
            end=dt(2015, 1, 10),
            freq='D'
        )
    )

    tsh.update(
        engine,
        ts_stock_obs,
        'stock_obs',
        'test',
        insertion_date=first_i_date
    )
    tsh.update(
        engine,
        ts_flow,
        'flow',
        'test',
        insertion_date=first_i_date
    )

    # second insertion
    ts_stock_obs = pd.Series(
        range(3),
        pd.date_range(
            start=dt(2015, 1, 1),
            end=dt(2015, 1, 3),
            freq='D'
        )
    )

    ts_flow = pd.Series(
        [0, 1] * 5,
        pd.date_range(
            start=dt(2015, 1, 1),
            end=dt(2015, 1, 10),
            freq='D'
        )
    )

    tsh.update(
        engine,
        ts_stock_obs,
        'stock_obs',
        'test',
        insertion_date=second_i_date
    )
    tsh.update(
        engine,
        ts_flow,
        'flow',
        'test',
        insertion_date=second_i_date
    )

    tsh.register_formula(
        engine,
        'ts_stock_fcst',
        '(integration "stock_obs" "flow")'
    )
    meta = tsh.internal_metadata(engine, 'ts_stock_fcst')
    assert meta == {
        'contenthash': '3dc8c4510650147caf7096761a1a4fece5b708ce',
        'formula': '(integration "stock_obs" "flow")',
        'index_dtype': '<M8[ns]',
        'index_type': 'datetime64[ns]',
        'tzaware': False,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

    ts_stock_fcst = tsh.get(engine, 'ts_stock_fcst')

    assert_df("""
2015-01-01    0.0
2015-01-02    1.0
2015-01-03    2.0
""", tsh.get(engine, 'stock_obs'))

    assert_df("""
2015-01-01    0.0
2015-01-02    1.0
2015-01-03    0.0
2015-01-04    1.0
2015-01-05    0.0
2015-01-06    1.0
2015-01-07    0.0
2015-01-08    1.0
2015-01-09    0.0
2015-01-10    1.0
""", tsh.get(engine, 'flow'))

    assert_df("""
2015-01-01    0.0
2015-01-02    1.0
2015-01-03    2.0
2015-01-04    3.0
2015-01-05    3.0
2015-01-06    4.0
2015-01-07    4.0
2015-01-08    5.0
2015-01-09    5.0
2015-01-10    6.0
""", ts_stock_fcst)

    # test of bounds with different overlaps with the integral and differential series
    assert 9 == len(tsh.get(engine, 'ts_stock_fcst', from_value_date=dt(2015, 1, 2)))
    assert 5 == len(tsh.get(engine, 'ts_stock_fcst', from_value_date=dt(2015, 1, 6)))
    assert 3 == len(tsh.get(engine, 'ts_stock_fcst', to_value_date=dt(2015, 1, 3)))
    assert 6 == len(tsh.get(engine, 'ts_stock_fcst', to_value_date=dt(2015, 1, 6)))
    assert 2 == len(tsh.get(engine, 'ts_stock_fcst',
                            from_value_date=dt(2015, 1, 2),
                            to_value_date=dt(2015, 1, 3)))
    assert 9 == len(tsh.get(engine, 'ts_stock_fcst',
                            from_value_date=dt(2015, 1, 2),
                            to_value_date=dt(2015, 1, 25)))
    assert 4 == len(tsh.get(engine, 'ts_stock_fcst',
                            from_value_date=dt(2015, 1, 6),
                            to_value_date=dt(2015, 1, 9)))

    # revision_date
    assert_df("""
2015-01-06     7.0
2015-01-07     8.0
2015-01-08    10.0
2015-01-09    11.0
2015-01-10    13.0
""", tsh.get(
    engine,
    'ts_stock_fcst',
    from_value_date=dt(2015, 1, 6),
    revision_date=pd.Timestamp(dt(2015, 1, 1, 12), tz='UTC')
))

    # tz-aware query -> don't fail
    tsh.get(
        engine,
        'ts_stock_fcst',
        from_value_date=pd.Timestamp('2015-1-6', tz='UTC')
    )

    # history
    hist = tsh.history(engine, 'ts_stock_fcst')

    assert_hist("""
insertion_date             value_date
2015-01-01 00:00:00+00:00  2015-01-01     0.0
                           2015-01-02     1.0
                           2015-01-03     2.0
                           2015-01-04     4.0
                           2015-01-05     5.0
                           2015-01-06     7.0
                           2015-01-07     8.0
                           2015-01-08    10.0
                           2015-01-09    11.0
                           2015-01-10    13.0
2015-01-02 00:00:00+00:00  2015-01-01     0.0
                           2015-01-02     1.0
                           2015-01-03     2.0
                           2015-01-04     3.0
                           2015-01-05     3.0
                           2015-01-06     4.0
                           2015-01-07     4.0
                           2015-01-08     5.0
                           2015-01-09     5.0
                           2015-01-10     6.0
""", hist)


def test_stock_fill(engine, tsh):
    base = pd.Series(
        [0, 0, 0, 0],
        index=[
            dt(2015, 1, 1),
            dt(2015, 1, 2),
            dt(2015, 1, 4),
            dt(2015, 1, 7),
        ]
    )
    tsh.update(engine, base, 'int-fill', 'test')

    temp = pd.Series(
        range(1, 10),
        index=pd.date_range(
            start=dt(2015, 1, 1),
            end=dt(2015, 1, 9),
            freq='D'
        )
    )
    tsh.update(engine, temp, 'temp', 'test')

    tsh.register_formula(
        engine,
        'ts_cumul_base',
        '(integration "int-fill" "temp")'
    )

    assert_df("""
2015-01-01    0.0
2015-01-02    0.0
2015-01-04    0.0
2015-01-07    0.0
""", tsh.get(engine, 'int-fill'))

    # by default, the integration is only done after the last value
    # of the stock
    assert_df("""
2015-01-01     0.0
2015-01-02     0.0
2015-01-04     0.0
2015-01-07     0.0
2015-01-08     8.0
2015-01-09    17.0
""", tsh.get(engine, 'ts_cumul_base'))

    tsh.register_formula(
        engine,
        'ts_filled_gap',
        '(integration "int-fill" "temp" #:fill #t)'
    )

    # the series is equal to the stock value when it exists
    # and integrates the flow in the in-beetween
    # it can simulate a tank, regulary emptied (when ts_stock=0)
    # very usefull to calculate the hdd reseted each year (ts_stock=0)
    chunks_limit = compute_bounds(
        tsh.get(engine, 'int-fill').index,
        tsh.get(engine, 'temp').index
    )
    # 1st col: stock start
    # 2nd col: stock end
    # 3rd col: flow start
    # 4th col: flow end

    assert_df("""
           0          1          2          3
0 2015-01-01 2015-01-02 2015-01-03 2015-01-03
1 2015-01-04 2015-01-04 2015-01-05 2015-01-06
2 2015-01-07 2015-01-07 2015-01-08 2015-01-09
""", pd.DataFrame(chunks_limit))

    result = pd.concat(
        [
            tsh.get(engine, 'int-fill'),
            tsh.get(engine, 'temp'),
            tsh.get(engine, 'ts_filled_gap')
        ],
        axis=1
    )

    assert_df("""
            int-fill  temp  ts_filled_gap
2015-01-01       0.0   1.0            0.0
2015-01-02       0.0   2.0            0.0
2015-01-03       NaN   3.0            3.0
2015-01-04       0.0   4.0            0.0
2015-01-05       NaN   5.0            5.0
2015-01-06       NaN   6.0           11.0
2015-01-07       0.0   7.0            0.0
2015-01-08       NaN   8.0            8.0
2015-01-09       NaN   9.0           17.0
""", result)

    # request where stock is out of range:
    stock1 = tsh.get(
        engine,
        'ts_cumul_base',
        from_value_date=dt(2015, 1, 8),
        to_value_date=dt(2015, 1, 9),
    )

    stock2 = tsh.get(
        engine,
        'ts_filled_gap',
        from_value_date=dt(2015, 1, 8),
        to_value_date=dt(2015, 1, 9),
    )

    # on the tail, the two formulas (with and without fill=True)
    # are expected to be the same
    assert stock1.equals(stock2)
    assert_df("""
2015-01-08     8.0
2015-01-09    17.0
""", stock1)

    # unusual case where the stock series goes further in the future
    # than the flow series
    base = pd.Series(
        [0, 0],
        index=[
            dt(2015, 1, 1),
            dt(2015, 1, 10),
        ]
    )
    tsh.update(engine, base, 'long-stock', 'test')

    temp = pd.Series(
        range(1, 6),
        index=pd.date_range(
            start=dt(2015, 1, 1),
            end=dt(2015, 1, 5),
            freq='D'
        )
    )
    tsh.update(engine, temp, 'short-flow', 'test')

    tsh.register_formula(
        engine,
        'ts-cumul',
        '(integration "long-stock" "short-flow" #:fill #t)'
    )

    # basically, the stock after the last flow values are ignored
    chunks_limit = compute_bounds(
        tsh.get(engine, 'long-stock').index,
        tsh.get(engine, 'short-flow').index
    )
    assert_df("""
           0          1          2          3
0 2015-01-01 2015-01-01 2015-01-02 2015-01-05
""", pd.DataFrame(chunks_limit))

    stock3 = tsh.get(
        engine,
        'ts-cumul'
    )

    assert_df("""
2015-01-01     0.0
2015-01-02     2.0
2015-01-03     5.0
2015-01-04     9.0
2015-01-05    14.0
""", stock3)

    # fill option when there is nothing to fill
    # we integrate a series on itself
    tsh.register_formula(
        engine,
        'nothing_to_fill',
        '(integration "temp" "temp" #:fill #t)'
    )

    ts = tsh.get(engine, 'nothing_to_fill')
    assert_df("""
2015-01-01    1.0
2015-01-02    2.0
2015-01-03    3.0
2015-01-04    4.0
2015-01-05    5.0
2015-01-06    6.0
2015-01-07    7.0
2015-01-08    8.0
2015-01-09    9.0
""", ts)

    # fill with the pattern:
    # stock ** **
    # fill  *****

    hollow_stock = tsh.get(engine, 'temp')
    hollow_stock.iloc[3] = np.nan
    tsh.update(
        engine,
        hollow_stock,
        'hollow_stock',
        'crazy_analyst'
    )

    tsh.register_formula(
        engine,
        'corner-case-36',
        '(integration "hollow_stock" "temp" #:fill #t))'
    )

    assert_df("""
2015-01-01    1.0
2015-01-02    2.0
2015-01-03    3.0
2015-01-04    7.0
2015-01-05    5.0
2015-01-06    6.0
2015-01-07    7.0
2015-01-08    8.0
2015-01-09    9.0
""", tsh.get(engine, 'corner-case-36'))


def test_stock_bounds(engine, tsh):
    ts_flow = pd.Series(
        [10, 11, 12, 13, 14],
        pd.date_range(
            start=dt(2015, 1, 1),
            end=dt(2015, 1, 5),
            freq='D'
        )
    )
    tsh.update(
        engine,
        ts_flow,
        'ts-flow',
        'test'
    )

    ts_stock_obs = pd.Series(
        range(3),
        pd.date_range(
            start=dt(2015, 1, 1),
            end=dt(2015, 1, 3),
            freq='D'
        )
    )

    tsh.update(
        engine,
        ts_stock_obs,
        'ts-stock-obs',
        'test'
    )
    tsh.register_formula(
        engine,
        'ts-stock',
        '(integration "ts-stock-obs" "ts-flow")'
    )
    all = tsh.get(
        engine,
        'ts-stock',
        from_value_date=dt(2015, 1, 1),
        to_value_date=dt(2015, 1, 5)
    )
    assert_df("""
2015-01-01     0.0
2015-01-02     1.0
2015-01-03     2.0
2015-01-04    15.0
2015-01-05    29.0
    """, all)

    # let's request data with bounds BEFORE the last value of observed stock
    bounded = tsh.get(
        engine,
        'ts-stock',
        to_value_date=dt(2015, 1, 2)
    )

    assert_df("""
2015-01-01    0.0
2015-01-02    1.0
""", bounded)


def test_more_integration_test(engine, tsh):
    # explicit combination bounds * fill option
    # the stock has the following pattern:
    # --xx--xx--xx--
    # and the values are negatives
    # while the flow:
    # xxxxxxxxxxxxxx
    # with positive values
    # we will try to request the integration with the bounds falling in
    # different place in this pattern
    ts = pd.Series(
        range(14),
        pd.date_range(
            start=dt(2022, 1, 1),
            end=dt(2022, 1, 14),
            freq='D'
        )
    )

    tsh.update(engine, ts, 'ts-diff', 'test')

    pattern = [True, True, False, False]
    ts = ts.loc[[False, False] + pattern * 3]
    assert 6 == len(ts)

    tsh.update(engine, -ts, 'ts-int', 'test')
    tsh.register_formula(
        engine,
        'plain-integration',
        '(integration "ts-int" "ts-diff"))'
    )

    tsh.register_formula(
        engine,
        'integration-with-fill',
        '(integration "ts-int" "ts-diff" #:fill #t))'
    )

    assert_df("""
2022-01-03    -2.0
2022-01-04    -3.0
2022-01-07    -6.0
2022-01-08    -7.0
2022-01-11   -10.0
2022-01-12   -11.0
2022-01-13     1.0
2022-01-14    14.0
""", tsh.get(engine, 'plain-integration'))

    assert_df("""
2022-01-03    -2.0
2022-01-04    -3.0
2022-01-05     1.0
2022-01-06     6.0
2022-01-07    -6.0
2022-01-08    -7.0
2022-01-09     1.0
2022-01-10    10.0
2022-01-11   -10.0
2022-01-12   -11.0
2022-01-13     1.0
2022-01-14    14.0
""", tsh.get(engine, 'integration-with-fill'))

    # first case: no stock present in the requested bounds
    # (the integration operator will look after the last stock
    # avaiable and integrate from there, with the same behaviour with
    # or without fill)
    fvd = dt(2022, 1, 13)
    tvd = dt(2022, 1, 14)

    tsp = tsh.get(
        engine,
        'plain-integration',
        from_value_date=fvd,
        to_value_date=tvd
    )
    tsf = tsh.get(
        engine,
        'integration-with-fill',
        from_value_date=fvd,
        to_value_date=tvd
    )

    assert tsp.equals(tsf)
    assert 2 == len(tsp)

    # second case: the lower bounds capture a segment where
    # the stock is present
    # same thing for the upper bound
    # plain integration should return the unaltered stock
    # integration with fill should interpolate the holes in the stock
    fvd = dt(2022, 1, 7)
    tvd = dt(2022, 1, 12)

    tsp = tsh.get(
        engine,
        'plain-integration',
        from_value_date=fvd,
        to_value_date=tvd
    )
    tsf = tsh.get(
        engine,
        'integration-with-fill',
        from_value_date=fvd,
        to_value_date=tvd
    )

    assert_df("""
2022-01-07    -6.0
2022-01-08    -7.0
2022-01-11   -10.0
2022-01-12   -11.0
""", tsp)

    assert_df("""
2022-01-07    -6.0
2022-01-08    -7.0
2022-01-09     1.0
2022-01-10    10.0
2022-01-11   -10.0
2022-01-12   -11.0
""", tsf)

    # third case, same as before, but the upper bound lands where the
    # stock is absent
    fvd = dt(2022, 1, 6)
    tvd = dt(2022, 1, 12)

    tsp = tsh.get(
        engine,
        'plain-integration',
        from_value_date=fvd,
        to_value_date=tvd
    )
    tsf = tsh.get(
        engine,
        'integration-with-fill',
        from_value_date=fvd,
        to_value_date=tvd
    )

    assert_df("""
2022-01-07    -6.0
2022-01-08    -7.0
2022-01-11   -10.0
2022-01-12   -11.0
""", tsp)

    assert_df("""
2022-01-06     6.0
2022-01-07    -6.0
2022-01-08    -7.0
2022-01-09     1.0
2022-01-10    10.0
2022-01-11   -10.0
2022-01-12   -11.0
""", tsf)

    # fourth case: the lower case lands with an absent stock but some
    # stock before
    fvd = dt(2022, 1, 4)
    tvd = dt(2022, 1, 8)

    tsp = tsh.get(
        engine,
        'plain-integration',
        from_value_date=fvd,
        to_value_date=tvd
    )
    tsf = tsh.get(
        engine,
        'integration-with-fill',
        from_value_date=fvd,
        to_value_date=tvd
    )

    assert_df("""
2022-01-04   -3.0
2022-01-07   -6.0
2022-01-08   -7.0
""", tsp)

    # the results are unintuitive but coherent with the first case
    assert_df("""
2022-01-04   -3.0
2022-01-05    1.0
2022-01-06    6.0
2022-01-07   -6.0
2022-01-08   -7.0
""", tsf)
    # still coherent with case #1

    # last case: lower bound has no stock with no stock before but some flow
    fvd = dt(2022, 1, 2)
    tvd = dt(2022, 1, 8)

    tsp = tsh.get(
        engine,
        'plain-integration',
        from_value_date=fvd,
        to_value_date=tvd
    )
    tsf = tsh.get(
        engine,
        'integration-with-fill',
        from_value_date=fvd,
        to_value_date=tvd
    )

    assert_df("""
2022-01-03   -2.0
2022-01-04   -3.0
2022-01-07   -6.0
2022-01-08   -7.0
""", tsp)

    assert_df("""
2022-01-03   -2.0
2022-01-04   -3.0
2022-01-05    1.0
2022-01-06    6.0
2022-01-07   -6.0
2022-01-08   -7.0
""", tsf)


def test_integration_which_big_gap(engine, tsh):
    ts = pd.Series(
        range(10),
        pd.date_range(
            start=dt(2021, 1, 1),
            end=dt(2021, 1, 10),
            freq='D'
        )
    )

    tsh.update(engine, ts, 'ts-far', 'test')

    ts = pd.Series(
        range(732),
        pd.date_range(
            start=dt(2020, 1, 1),
            end=dt(2022, 1, 1),
            freq='D'
        )
    )
    tsh.update(engine, ts, 'ts-recent', 'test')

    tsh.register_formula(
        engine,
        'integration-gap',
        '(integration "ts-far" "ts-recent"))'
    )

    tsh.register_formula(
        engine,
        'integration-gap-with-fill',
        '(integration "ts-far" "ts-recent" #:fill #t))'
    )

    tsp = tsh.get(
        engine,
        'integration-gap',
        from_value_date=dt(2021, 12, 25),
        to_value_date=dt(2022, 1, 10)
    )

    tsf = tsh.get(
        engine,
        'integration-gap-with-fill',
        from_value_date=dt(2021, 12, 25),
        to_value_date=dt(2022, 1, 10)
    )

    assert_df("""
2021-12-25    191959.0
2021-12-26    192684.0
2021-12-27    193410.0
2021-12-28    194137.0
2021-12-29    194865.0
2021-12-30    195594.0
2021-12-31    196324.0
2022-01-01    197055.0
""", tsp)

    assert tsp.equals(tsf)


def test_conditional_operators(engine, tsh):
    # base series
    ts = pd.Series(
        range(5),
        index=pd.date_range(
            start=dt(2023, 1, 10),
            freq='D',
            periods=5
        )
    )
    tsh.update(engine, ts, 'series-cond', 'test')

    # series for comparison (index shifted with overlap)
    ts = pd.Series(
        [3.0] * 5,
        index=pd.date_range(
            start=dt(2023, 1, 8),
            freq='D',
            periods=5
        )
    )
    tsh.update(engine, ts, 'series-to-compare', 'test')

    # series to compare without overlap
    ts = pd.Series(
        [4.0] * 5,
        index=pd.date_range(
            start=dt(2023, 1, 2),
            freq='D',
            periods=5
        )
    )
    tsh.update(engine, ts, 'series-no-overlap', 'test')

    tsh.register_formula(
        engine,
        'cond0',
        '(> (series "series-cond") 3)'
    )
    assert_df("""
2023-01-10    0.0
2023-01-11    0.0
2023-01-12    0.0
2023-01-13    0.0
2023-01-14    1.0
""", tsh.get(engine, 'cond0'))

    tsh.register_formula(
        engine,
        'cond1',
        '(<= (series "series-cond") 3 #:true_value 3.14)'
    )
    assert_df("""
2023-01-10    3.14
2023-01-11    3.14
2023-01-12    3.14
2023-01-13    3.14
2023-01-14    0.00
""", tsh.get(engine, 'cond1'))

    tsh.register_formula(
        engine,
        'cond2',
        '(== (series "series-cond") 3 #:false_value -1)'
    )
    assert_df("""
2023-01-10   -1.0
2023-01-11   -1.0
2023-01-12   -1.0
2023-01-13    1.0
2023-01-14   -1.0
""", tsh.get(engine, 'cond2'))

    # the two series do not have the same length
    # the comparison return a result-series only
    # on the common part
    tsh.register_formula(
        engine,
        'cond3',
        '(>= (series "series-cond") (series "series-to-compare")))'
    )
    assert_df("""
2023-01-10    0.0
2023-01-11    0.0
2023-01-12    0.0
""", tsh.get(engine, 'cond3'))

    tsh.register_formula(
        engine,
        'cond4',
        '(<> (series "series-cond") (series "series-no-overlap")))'
    )

    assert len(tsh.get(engine, 'cond4')) == 0
