from datetime import datetime as dt

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
