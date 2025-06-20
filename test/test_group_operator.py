from datetime import datetime as dt

import pandas as pd

from tshistory.testutil import (
    assert_df,
    gengroup,
    utcdt
)


def test_group_add(engine, tsh):
    for name in ('group1', 'group2', 'group3'):
        tsh.group_delete(engine, name)

    df1 = gengroup(
        n_scenarios=3,
        from_date=dt(2025, 5, 1),
        length=5,
        freq='D',
        seed=2
    )

    colnames = ['a', 'b', 'c']
    df1.columns = colnames

    idate = utcdt(2025, 5, 1)

    tsh.group_update(engine, df1, 'group1', 'test', insertion_date=idate)

    tsh.group_update(engine, df1*2, 'group2', 'test', insertion_date=idate)

    tsh.group_update(engine, df1*3, 'group3', 'test', insertion_date=idate)


    tsh.register_group_formula(
        engine,
        'addgroup',
        '(group-add '
        '  (group "group1")'
        '  (group "group2")'
        '  (group "group3"))',
    )

    df = tsh.group_get(engine, 'addgroup')
    assert_df("""
               a     b     c
2025-05-01  12.0  18.0  24.0
2025-05-02  18.0  24.0  30.0
2025-05-03  24.0  30.0  36.0
2025-05-04  30.0  36.0  42.0
2025-05-05  36.0  42.0  48.0
""", df)

    limited = tsh.group_get(
        engine, 'addgroup',
        from_value_date=dt(2025, 5, 3),
        to_value_date=dt(2025, 5, 4)
    )
    assert_df("""
               a     b     c
2025-05-03  24.0  30.0  36.0
2025-05-04  30.0  36.0  42.0
""", limited)

    # make some history
    idate2 = utcdt(2025, 5, 2)
    df2 = gengroup(
        n_scenarios=3,
        from_date=dt(2025, 5, 1),
        length=5,
        freq='D',
        seed=3
    )

    colnames = ['a', 'b', 'c']
    df2.columns = colnames

    tsh.group_update(engine, df2*(-1), 'group1', 'test', insertion_date=idate2)

    tsh.group_update(engine, df2*(-3), 'group2', 'test', insertion_date=idate2)

    tsh.group_update(engine, df2*(-5), 'group3', 'test', insertion_date=idate2)

    df = tsh.group_get(engine, 'addgroup')
    assert_df("""
               a     b     c
2025-05-01 -27.0 -36.0 -45.0
2025-05-02 -36.0 -45.0 -54.0
2025-05-03 -45.0 -54.0 -63.0
2025-05-04 -54.0 -63.0 -72.0
2025-05-05 -63.0 -72.0 -81.0
""", df)

    df = tsh.group_get(engine, 'addgroup', revision_date=idate)
    assert_df("""
               a     b     c
2025-05-01  12.0  18.0  24.0
2025-05-02  18.0  24.0  30.0
2025-05-03  24.0  30.0  36.0
2025-05-04  30.0  36.0  42.0
2025-05-05  36.0  42.0  48.0
""", df)


def test_null_group_add(engine, tsh):
    tsh.register_group_formula(
        engine,
        'null-groupadd',
        '(group-add)'
    )
    df = tsh.group_get(engine, 'null-groupadd')
    assert len(df) == 0


def test_group_from_series(engine, tsh):
    ts1 = pd.Series(
        [1.0, -2, 0, -3, -2, 0, -3],
        index=pd.date_range(
            start=pd.Timestamp('2025-04-25'),
            freq='D',
            periods=7
        )
    )
    tsh.update(engine, ts1, 'series1', 'test')
    tsh.update(engine, ts1*2, 'series2', 'test')
    tsh.update(engine, ts1*(-2), 'series3', 'test')

    tsh.register_group_formula(
        engine,
        'group-from-series',
        '(group-from-series '
        '  (bind "scenario1" (series "series1"))'
        '  (bind "scenario2" (series "series2"))'
        '  (bind "scenario3" (series "series3"))'
        ')'
    )
    assert not tsh.group_internal_metadata(engine, 'group-from-series')['tzaware']

    df = tsh.group_get(engine, 'group-from-series')
    assert_df("""
            scenario1  scenario2  scenario3
2025-04-25        1.0        2.0       -2.0
2025-04-26       -2.0       -4.0        4.0
2025-04-27        0.0        0.0       -0.0
2025-04-28       -3.0       -6.0        6.0
2025-04-29       -2.0       -4.0        4.0
2025-04-30        0.0        0.0       -0.0
2025-05-01       -3.0       -6.0        6.0
""", df)

    tsh.update(engine, ts1[:2], 'short-series', 'test')
    tsh.update(engine, ts1.drop(ts1.index[[2]]), 'hole-series', 'test')

    # without fill option
    tsh.register_group_formula(
        engine,
        'group-from-series-holes',
        '(group-from-series '
        '  (bind "scenario1" (series "series1"))'
        '  (bind "short" (series "short-series"))'
        '  (bind "holes" (series "hole-series"))'
        ')'
    )

    df = tsh.group_get(engine, 'group-from-series-holes')
    assert_df("""
            scenario1  short  holes
2025-04-25        1.0    1.0    1.0
2025-04-26       -2.0   -2.0   -2.0
2025-04-27        0.0    NaN    NaN
2025-04-28       -3.0    NaN   -3.0
2025-04-29       -2.0    NaN   -2.0
2025-04-30        0.0    NaN    0.0
2025-05-01       -3.0    NaN   -3.0
""", df)

    # with fill option
    tsh.register_group_formula(
        engine,
        'group-from-series-fill',
        '(group-from-series '
        '  (bind "scenario1" (series "series1"))'
        '  (bind "short" (series "short-series" #:fill 0))'
        '  (bind "holes" (series "hole-series" #:fill "ffill"))'
        ')'
    )

    df = tsh.group_get(engine, 'group-from-series-fill')
    assert_df("""
            scenario1  short  holes
2025-04-25        1.0    1.0    1.0
2025-04-26       -2.0   -2.0   -2.0
2025-04-27        0.0    0.0   -2.0
2025-04-28       -3.0    0.0   -3.0
2025-04-29       -2.0    0.0   -2.0
2025-04-30        0.0    0.0    0.0
2025-05-01       -3.0    0.0   -3.0
""", df)

    # mixed freq
    ts1 = pd.Series(
        [1.0, -2, 0, -3, -2, 0, -3],
        index=pd.date_range(
            start=pd.Timestamp('2025-04-25'),
            freq='h',
            periods=7
        )
    )
    tsh.update(engine, ts1, 'series-hourly', 'test')

    tsh.register_group_formula(
        engine,
        'group-from-series-mixedfreq',
        '(group-from-series '
        '  (bind "scenario1" (series "series1" #:fill 0))'
        '  (bind "hourly" (series "series-hourly"))'
        ')'
    )

    df = tsh.group_get(engine, 'group-from-series-mixedfreq')
    assert_df("""
                     scenario1  hourly
2025-04-25 00:00:00        1.0     1.0
2025-04-25 01:00:00        0.0    -2.0
2025-04-25 02:00:00        0.0     0.0
2025-04-25 03:00:00        0.0    -3.0
2025-04-25 04:00:00        0.0    -2.0
2025-04-25 05:00:00        0.0     0.0
2025-04-25 06:00:00        0.0    -3.0
2025-04-26 00:00:00       -2.0     NaN
2025-04-27 00:00:00        0.0     NaN
2025-04-28 00:00:00       -3.0     NaN
2025-04-29 00:00:00       -2.0     NaN
2025-04-30 00:00:00        0.0     NaN
2025-05-01 00:00:00       -3.0     NaN
""", df)


def test_group_from_series_tzaware(engine, tsh):
    ts1 = pd.Series(
        [1.0, -2, 0, -3, -2, 0, -3],
        index=pd.date_range(
            start=pd.Timestamp('2025-04-25', tz='utc'),
            freq='D',
            periods=7
        )
    )
    tsh.update(engine, ts1, 'tz-series1', 'test')
    tsh.update(engine, ts1*2, 'tz-series2', 'test')
    tsh.update(engine, ts1*(-2), 'tz-series3', 'test')

    tsh.register_group_formula(
        engine,
        'tzaware-group-from-series',
        '(group-from-series '
        '  (bind "scenario1" (series "tz-series1"))'
        '  (bind "scenario2" (series "tz-series2"))'
        '  (bind "scenario3" (series "tz-series3"))'
        ')'
    )
    assert tsh.group_internal_metadata(engine, 'tzaware-group-from-series')['tzaware']


def test_groupaddseries(engine, tsh):
    df1 = gengroup(
        n_scenarios=3,
        from_date=dt(2025, 5, 1),
        length=5,
        freq='D',
        seed=2
    )

    colnames = ['a', 'b', 'c']
    df1.columns = colnames

    idate = utcdt(2025, 5, 3)

    tsh.group_replace(engine, df1*2, 'group2', 'test', insertion_date=idate)

    ts1 = pd.Series(
        [1.0, -2, 0, -3, -2, 0, -3],
        index=pd.date_range(
            start=pd.Timestamp('2025-04-25'),
            freq='D',
            periods=7
        )
    )
    tsh.update(engine, ts1, 'series1', 'test')

    tsh.register_group_formula(
        engine,
        'group-add-series',
        '''(group-add-series (group "group2") (series "series1"))'''
    )

    df = tsh.group_get(engine, 'group-add-series')

    assert_df("""
              a    b    c
2025-05-01  1.0  3.0  5.0
""", df)

    # with fill option
    tsh.register_group_formula(
        engine,
        'group-add-series',
        '''(group-add-series (group "group2") (series "series1" #:fill 0))'''
    )

    df = tsh.group_get(engine, 'group-add-series')

    assert_df("""
               a     b     c
2025-05-01   1.0   3.0   5.0
2025-05-02   6.0   8.0  10.0
2025-05-03   8.0  10.0  12.0
2025-05-04  10.0  12.0  14.0
2025-05-05  12.0  14.0  16.0
""", df)


