from datetime import datetime
import pandas as pd
import pytest

from psyl import lisp
from tshistory.testutil import (
    assert_df,
    assert_hist,
    gengroup,
    utcdt
)

from tshistory_formula.tsio import timeseries
from tshistory_formula.registry import (
    func,
    finder,
    insertion_dates,
    metadata
)


def test_eval_formula(tsx):
    tsx.update(
        'test-eval',
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(
                pd.Timestamp('2022-1-1', tz='utc'),
                periods=3,
                freq='D')
        ),
        'Babar',
        insertion_date=pd.Timestamp('2022-1-5', tz='utc')
    )

    tsx.update(
        'test-eval',
        pd.Series(
            [1, 2, 3, 4, 5],
            index=pd.date_range(
                pd.Timestamp('2022-1-1', tz='utc'),
                periods=5,
                freq='D')
        ),
        'Babar',
        insertion_date=pd.Timestamp('2022-1-6', tz='utc')
    )

    ts = tsx.eval_formula(
        '(+ 1 (series "test-eval"))',
        from_value_date=pd.Timestamp('2022-1-2', tz='utc'),
        to_value_date=pd.Timestamp('2022-1-4', tz='utc')
    )
    assert_df("""
2022-01-02 00:00:00+00:00    3.0
2022-01-03 00:00:00+00:00    4.0
2022-01-04 00:00:00+00:00    5.0
""", ts)

    ts = tsx.eval_formula(
        '(+ 1 (series "test-eval"))',
        revision_date=pd.Timestamp('2022-1-5', tz='utc')
    )
    assert_df("""
2022-01-01 00:00:00+00:00    2.0
2022-01-02 00:00:00+00:00    3.0
2022-01-03 00:00:00+00:00    4.0
""", ts)

    with pytest.raises(SyntaxError):
        tsx.eval_formula(
            '(+ 1 i am borked'
        )

    with pytest.raises(TypeError) as err:
        tsx.eval_formula(
            '(+ 1 (fake-operator "test-eval"))'
        )
    assert err.value.args[0] == (
        'expression `(fake-operator "test-eval")` '
        'refers to unknown operator `fake-operator`'
    )

    with pytest.raises(TypeError) as err:
        tsx.eval_formula(
            '(+ "1" (series "test-eval"))'
        )
    assert err.value.args[0] == "'1' not of <class 'numbers.Number'>"


def test_bogus_formula(tsx):
    with pytest.raises(TypeError) as err:
        tsx.register_formula(
            'bogus',
            '(resample "nope")'
        )
    print(err)


def test_local_formula_remote_series(tsa):
    rtsh = timeseries('test-mapi-2')
    rtsh.update(
        tsa.engine,
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(pd.Timestamp('2020-1-1'), periods=3, freq='H'),
        ),
        'remote-series',
        'Babar',
        insertion_date=pd.Timestamp('2020-1-1', tz='UTC')
    )

    tsa.register_formula(
        'test-localformula-remoteseries',
        '(+ 1 (series "remote-series"))'
    )

    ts = tsa.get('test-localformula-remoteseries')
    assert_df("""
2020-01-01 00:00:00    2.0
2020-01-01 01:00:00    3.0
2020-01-01 02:00:00    4.0
""", ts)

    hist = tsa.history('test-localformula-remoteseries')
    assert_hist("""
insertion_date             value_date         
2020-01-01 00:00:00+00:00  2020-01-01 00:00:00    2.0
                           2020-01-01 01:00:00    3.0
                           2020-01-01 02:00:00    4.0
""", hist)

    f = tsa.formula('test-localformula-remoteseries')
    assert f == '(+ 1 (series "remote-series"))'

    none = tsa.formula('nosuchformula')
    assert none is None

    # altsource formula
    rtsh.register_formula(
        tsa.engine,
        'remote-formula-remote-series',
        '(+ 2 (series "remote-series"))'
    )
    f = tsa.formula('remote-formula-remote-series')
    assert f == '(+ 2 (series "remote-series"))'

    assert_df("""
2020-01-01 00:00:00    3.0
2020-01-01 01:00:00    4.0
2020-01-01 02:00:00    5.0
""", tsa.get('remote-formula-remote-series'))


    rtsh.register_formula(
        tsa.engine,
        'remote-formula-local-formula',
        '(+ 3 (series "remote-formula-remote-series"))'
    )
    f = tsa.formula('remote-formula-local-formula')
    assert f == '(+ 3 (series "remote-formula-remote-series"))'

    ts = tsa.get('remote-formula-local-formula')
    assert_df("""
2020-01-01 00:00:00    6.0
2020-01-01 01:00:00    7.0
2020-01-01 02:00:00    8.0
""", ts)

    expanded = tsa.formula('remote-formula-local-formula', expanded=True)
    assert expanded == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (+ 3 (+ 2 (series "remote-series")))'
        ')'
    )
    expanded = tsa.formula(
        'remote-formula-local-formula',
        display=True,
        expanded=True
    )
    assert expanded == (
        '(+ 3 (+ 2 (series "remote-series")))'
    )


def test_formula_components(tsa):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-6-1'), freq='D', periods=3)
    )
    tsa.update(
        'component-a',
        series,
        'Babar'
    )
    tsa.update(
        'component-b',
        series,
        'Celeste'
    )

    assert tsa.formula_components('component-a') is None

    form = '(add (series "component-a") (series "component-b"))'
    tsa.register_formula(
        'show-components',
        form
    )

    components = tsa.formula_components('show-components')
    parsed = lisp.parse(form)
    assert components == {
        'show-components': ['component-a', 'component-b']
    }

    tsa.register_formula(
        'show-components-squared',
        '(add (* 2 (series "show-components")) (series "component-b"))'
    )
    components = tsa.formula_components(
        'show-components-squared',
        expanded=True
    )
    assert components == {
        'show-components-squared': [
            {'show-components':
             [
                 'component-a',
                 'component-b'
             ]
            },
            'component-b'
        ]
    }

    # formula referencing a remote formula
    rtsh = timeseries('test-mapi-2')
    rtsh.update(
        tsa.engine,
        series,
        'remote-series-compo',
        'Babar',
        insertion_date=pd.Timestamp('2020-1-1', tz='UTC')
    )
    rtsh.register_formula(
        tsa.engine,
        'remote-formula',
        '(+ 1 (series "remote-series-compo"))'
    )

    tsa.register_formula(
        'compo-with-remoteseries',
        '(add (series "show-components-squared") (series "remote-formula"))'
    )
    components = tsa.formula_components(
        'compo-with-remoteseries',
        expanded=True
    )
    assert components == {
        'compo-with-remoteseries': [
            {'show-components-squared': [
                {'show-components': ['component-a',
                                     'component-b']
                },
                'component-b'
            ]},
            {'remote-formula': ['remote-series-compo']}
        ]
    }

    # pure remote formula
    components = tsa.formula_components(
        'remote-formula',
        expanded=True
    )
    assert components == {
        'remote-formula': [
            'remote-series-compo'
        ]
    }

    idates = tsa.insertion_dates('remote-formula')
    assert len(idates) == 1
    assert idates[0] == pd.Timestamp('2020-01-01 00:00:00+0000', tz='UTC')
    idates = tsa.insertion_dates('compo-with-remoteseries')
    assert len(idates) == 3


def test_formula_remote_autotrophic(tsa, engine):
    from tshistory_formula.registry import func, metadata
    from tshistory_formula.tsio import timeseries as pgseries

    @func('customseries')
    def customseries() -> pd.Series:
        return pd.Series(
            [1.0, 2.0, 3.0],
            index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
        )

    @metadata('customseries')
    def metadata(cn, tsh, tree):
        return {
            tree[0]: {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

    rtsh = pgseries('test-mapi-2')
    with engine.begin() as cn:
        rtsh.register_formula(
            cn,
            'autotrophic',
            '(customseries)',
        )

    tsa.register_formula(
        'remote-series',
        '(series "autotrophic")'
    )

    # bw compat
    assert tsa.metadata('remote-series', True) == {
        'contenthash': 'e74bac3752e17245340a7d0cbeb2bdfccdbf3953',
        'formula': '(series "autotrophic")',
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

    assert tsa.metadata('remote-series') == {}

    assert tsa.internal_metadata('remote-series') == {
        'contenthash': 'e74bac3752e17245340a7d0cbeb2bdfccdbf3953',
        'formula': '(series "autotrophic")',
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

    assert_df("""
2019-01-01 00:00:00+00:00    1.0
2019-01-02 00:00:00+00:00    2.0
2019-01-03 00:00:00+00:00    3.0
""", tsa.get('remote-series'))

    idates = tsa.insertion_dates('remote-series')
    # autotrophic series lack an `insertion_dates` protocol
    assert idates == []

    assert tsa.type('remote-series') == 'formula'
    assert tsa.othersources.sources[0].tsa.type('autotrophic') == 'formula'
    assert tsa.type('autotrophic') == 'formula'


def test_formula_components_wall(tsa):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-6-1'), freq='D', periods=3)
    )
    tsa.update(
        'comp-a',
        series,
        'Babar'
    )
    tsa.update(
        'comp-b',
        series,
        'Celeste'
    )
    tsa.update(
        'comp-c',
        series,
        'Arthur'
    )

    tsa.register_formula(
        'b-plus-c',
        '(add (series "comp-b") (series "comp-c"))'
    )

    @func('opaque-components', auto=True)
    def custom(__interpreter__,
               __from_value_date__,
               __to_value_date__,
               __revision_date__,
               s1name: str,
               s2name: str) -> pd.Series:
        i = __interpreter__
        s1 = i.get(i.cn, s1name)
        s2 = i.get(i.cn, s2name)
        return s1 + s2


    @finder('opaque-components')
    def custom(cn, tsh, tree):
        return {
            tree[1]: tree,
            tree[2]: tree
        }

    tsa.register_formula(
        'wall',
        '(opaque-components "comp-a" "b-plus-c")'
    )

    comp = tsa.formula_components('wall')
    assert comp == {
        'wall': ['comp-a', 'b-plus-c']
    }

    comp = tsa.formula_components('wall', expanded=True)
    assert comp == {
        'wall': [
            'comp-a',
            {'b-plus-c': [
                'comp-b',
                'comp-c'
            ]}
        ]
    }


def test_autotrophic_idates(tsx):
    # using the fallback path through .history

    @func('autotrophic', auto=True)
    def custom(__interpreter__,
               __from_value_date__,
               __to_value_date__,
               __revision_date__) -> pd.Series:
        return pd.Series(
            [1, 2, 3],
            pd.date_range(utcdt(2020, 1, 1), periods=1, freq='D')
        )

    @finder('autotrophic')
    def custom(cn, tsh, tree):
        return {
            'I HAVE A NAME FOR DISPLAY PURPOSES': tree
        }

    tsx.register_formula(
        'autotrophic-idates',
        '(autotrophic)'
    )

    idates = tsx.insertion_dates('autotrophic-idates')
    assert idates == []


def test_autotrophic_idates2(tsx):
    @func('auto2', auto=True)
    def custom(__interpreter__,
               __from_value_date__,
               __to_value_date__,
               __revision_date__) -> pd.Series:
        return pd.Series(
            [1, 2, 3],
            pd.date_range(utcdt(2020, 1, 1), periods=1, freq='D')
        )

    @finder('auto2')
    def custom(cn, tsh, tree):
        return {
            'I HAVE A NAME FOR DISPLAY PURPOSES': tree
        }

    @insertion_dates('auto2')
    def custom(cn, tsh, tree,
               from_insertion_date, to_insertion_date,
               from_value_date, to_value_date):
        dates = [
            pd.Timestamp('2020-1-1', tz='utc'),
            pd.Timestamp('2020-1-2', tz='utc')
        ]
        fromdate = from_insertion_date or pd.Timestamp('1900-1-1', tz='UTC')
        todate = to_insertion_date or pd.Timestamp('2100-1-1', tz='UTC')
        return filter(lambda d: fromdate <= d <= todate, dates)

    tsx.register_formula(
        'autotrophic-idates-2',
        '(auto2)'
    )

    idates = tsx.insertion_dates('autotrophic-idates-2')
    assert idates == [
        pd.Timestamp('2020-1-1', tz='utc'),
        pd.Timestamp('2020-1-2', tz='utc')
    ]

    idates = tsx.insertion_dates(
        'autotrophic-idates-2',
        pd.Timestamp('2020-1-2', tz='UTC')
    )
    assert idates == [
        pd.Timestamp('2020-1-2', tz='utc')
    ]

    idates = tsx.insertion_dates(
        'autotrophic-idates-2',
        to_insertion_date=pd.Timestamp('2020-1-1', tz='UTC')
    )
    assert idates == [
        pd.Timestamp('2020-1-1', tz='utc')
    ]


# groups

def test_group_formula(tsa):
    df = gengroup(
        n_scenarios=3,
        from_date=datetime(2015, 1, 1),
        length=5,
        freq='D',
        seed=2
    )

    df.columns = ['a', 'b', 'c']

    tsa.group_replace('groupa', df, 'test')

    plain_ts = pd.Series(
        [1] * 7,
        index=pd.date_range(
            start=datetime(2014, 12, 31),
            freq='D',
            periods=7,
        )
    )

    tsa.update('plain_tsa', plain_ts, 'Babar')

    # start to test

    formula = (
        '(group-add '
        '  (group "groupa") '
        '  (* -1 '
        '    (series "plain_tsa")))'
    )

    tsa.register_group_formula(
        'difference',
        formula
    )
    df = tsa.group_get('difference')
    assert_df("""
              a    b    c
2015-01-01  1.0  2.0  3.0
2015-01-02  2.0  3.0  4.0
2015-01-03  3.0  4.0  5.0
2015-01-04  4.0  5.0  6.0
2015-01-05  5.0  6.0  7.0
""", df)

    assert tsa.group_metadata('difference') == {}
    assert tsa.group_metadata('difference', all=True) == {
        'tzaware': False,
        'index_type': 'datetime64[ns]',
        'value_type': 'float64',
        'index_dtype': '<M8[ns]',
        'value_dtype': '<f8'
    }

    # formula of formula
    # we add the same series that was substracted,
    # hence we msut retrieve the original dataframe group1
    formula = (
        '(group-add '
        '  (group "difference")'
        '  (series "plain_tsa"))'
    )

    tsa.register_group_formula(
        'roundtripeda',
        formula
    )

    df_roundtrip = tsa.group_get('roundtripeda')
    df_original = tsa.group_get('groupa')

    assert df_roundtrip.equals(df_original)


def test_group_bound_formula(tsa):
    temp = pd.Series(
        [12, 13, 14],
        index=pd.date_range(utcdt(2021, 1, 1), freq='D', periods=3)
    )
    wind = pd.Series(
        [.1, .1, .1],
        index=pd.date_range(utcdt(2021, 1, 1), freq='D', periods=3)
    )

    tsa.update('base-temp', temp, 'Babar')
    tsa.update('base-wind', wind, 'Celeste')

    tsa.register_formula(
        'hijacked',
        '(add (series "base-temp") (series "base-wind"))'
    )

    df1 = gengroup(
        n_scenarios=2,
        from_date=utcdt(2021, 1, 1),
        length=3,
        freq='D',
        seed=0
    )
    tsa.group_replace(
        'temp-ens',
        df1,
        'Arthur'
    )
    assert_df("""
                           0  1
2021-01-01 00:00:00+00:00  0  1
2021-01-02 00:00:00+00:00  1  2
2021-01-03 00:00:00+00:00  2  3
""", df1)

    df2 = gengroup(
        n_scenarios=2,
        from_date=utcdt(2021, 1, 1),
        length=3,
        freq='D',
        seed=1
    )
    tsa.group_replace(
        'wind-ens',
        df2,
        'Zéphir'
    )
    assert_df("""
                           0  1
2021-01-01 00:00:00+00:00  1  2
2021-01-02 00:00:00+00:00  2  3
2021-01-03 00:00:00+00:00  3  4
""", df2)

    binding = pd.DataFrame(
        [
            ['base-temp', 'temp-ens', 'meteo'],
            ['base-wind', 'wind-ens', 'meteo'],
        ],
        columns=('series', 'group', 'family')
    )

    with pytest.raises(AssertionError):
        tsa.register_formula_bindings(
            'hijacking',
            'hijacked',
            pd.DataFrame(
                [
                    ['NOPE', 'temp-ens', 'meteo'],
                    ['base-wind', 'wind-ens', 'meteo'],
                ],
                columns=('series', 'group', 'family')
            )
        )

    with pytest.raises(AssertionError):
        tsa.register_formula_bindings(
            'hijacking',
            'hijacked',
            pd.DataFrame(
                [
                    ['base-temp', 'temp-ens', 'meteo'],
                    ['base-wind', 'NOPE', 'meteo'],
                ],
                columns=('series', 'group', 'family')
            )
        )

    tsa.register_formula_bindings(
        'hijacking',
        'hijacked',
        binding
    )

    ts = tsa.get('hijacked')
    assert_df("""
2021-01-01 00:00:00+00:00    12.1
2021-01-02 00:00:00+00:00    13.1
2021-01-03 00:00:00+00:00    14.1
""", ts)

    df = tsa.group_get('hijacking')
    assert_df("""
                             0    1
2021-01-01 00:00:00+00:00  1.0  3.0
2021-01-02 00:00:00+00:00  3.0  5.0
2021-01-03 00:00:00+00:00  5.0  7.0
""", df)

    assert tsa.group_exists('hijacking')
    assert tsa.group_type('hijacking') == 'bound'

    cat = list(tsa.group_catalog().values())[0]
    assert ('hijacking', 'bound') in cat

    assert tsa.group_metadata('hijacking') == {}
    assert tsa.group_metadata('hijacking', all=True) == {
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }
    tsa.update_group_metadata('hijacking', {'foo': 'bar'})
    assert tsa.group_metadata('hijacking') == {'foo': 'bar'}
    assert tsa.group_metadata('hijacking', all=True) == {
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64',
        'foo': 'bar'
    }

    tsa.group_delete('hijacking')
    assert not tsa.group_exists('hijacking')

    assert tsa.group_metadata('hijacking') is None


def test_more_group_errors(tsx):
    tsx.delete('toto')
    tsx.delete('tata')

    df = gengroup(
        n_scenarios=3,
        from_date=datetime(2015, 1, 1),
        length=5,
        freq='D',
        seed=2
    )

    tsx.group_replace('conflict-name-primary', df, 'toto')
    tsx.register_group_formula('conflict-name-formula', '(group "toto")')

    # toto does not exist: for homogneity purposes an error should be raised

    # bad bindings
    with pytest.raises(Exception) as excinfo:
        tsx.register_formula_bindings(
            'conflict-name-binding',
            'toto',
            pd.DataFrame()
        )
    assert str(excinfo.value) == 'bindings must have `series` `groups` and `family` columns'

    with pytest.raises(Exception) as excinfo:
        tsx.register_formula_bindings(
            'conflict-name-binding',
            'toto',
            pd.DataFrame([['a', 'b', 'c']], columns=('series', 'group', 'family'))
        )
    assert str(excinfo.value) == '`toto` is not a formula'

    tsx.register_formula(
        'toto',
        '(constant 42.5 (date "1900-1-1") (date "2039-12-31") "D" (date "1900-1-1"))'
    )

    with pytest.raises(Exception) as excinfo:
        tsx.register_formula_bindings(
            'conflict-name-binding',
            'toto',
            pd.DataFrame([['a', 'b', 'c']], columns=('series', 'group', 'family'))
        )
    assert str(excinfo.value) == 'Group `b` does not exist.'

    for name in 'abc':
        tsx.group_replace(
            name,
            df,
            'Babar'
        )
        tsx.update(
            name,
            pd.Series(
                [1, 2, 3],
                pd.date_range(pd.Timestamp('2021-1-1'), freq='D', periods=3)
            ),
            'Babar'
        )

    tsx.register_formula_bindings(
        'conflict-name-binding',
        'toto',
        pd.DataFrame([['a', 'b', 'c']], columns=('series', 'group', 'family'))
    )

    with pytest.raises(Exception) as excinfo:
        tsx.group_replace('conflict-name-formula', df, 'toto')
    assert str(excinfo.value) == (
        'cannot group-replace `conflict-name-formula`: this name has type `formula`'
    )

    with pytest.raises(Exception) as excinfo:
        tsx.group_replace('conflict-name-binding', df, 'toto')
    assert str(excinfo.value) == (
        'cannot group-replace `conflict-name-binding`: this name has type `bound`'
    )

    with pytest.raises(Exception) as excinfo:
        tsx.register_group_formula('conflict-name-primary', '(group "toto")')
    assert str(excinfo.value) == (
        'cannot register formula `conflict-name-primary`: already a `primary`'
    )

    with pytest.raises(Exception) as excinfo:
        tsx.register_group_formula('conflict-name-binding', '(group "toto")')
    assert str(excinfo.value) == (
        'cannot register formula `conflict-name-binding`: already a `bound`'
    )

    with pytest.raises(Exception) as excinfo:
        tsx.register_formula_bindings(
            'conflict-name-primary',
            'toto',
            pd.DataFrame([['a', 'b', 'c']], columns=('series', 'group', 'family'))
        )
    assert str(excinfo.value) == (
        'cannot bind `conflict-name-primary`: already a primary'
    )
    with pytest.raises(Exception) as excinfo:
        tsx.register_formula_bindings(
            'conflict-name-formula',
            'toto',
            pd.DataFrame([['a', 'b', 'c']], columns=('series', 'group', 'family'))
        )
    assert str(excinfo.value) == (
        'cannot bind `conflict-name-formula`: already a formula'
    )

    # overrides
    tsx.register_group_formula('conflict-name-formula', '(group "tata")')

    # goes smoothly
    tsx.register_formula(
        'tata',
        '(constant 42.5 (date "1900-1-1") (date "2039-12-31") "D" (date "1900-1-1"))'
    )
    tsx.register_formula_bindings(
        'conflict-name-binding',
        'tata',
        pd.DataFrame([['a', 'b', 'c']], columns=('series', 'group', 'family'))
    )

