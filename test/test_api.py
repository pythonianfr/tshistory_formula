from datetime import datetime
import pandas as pd
import pytest

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
    insertion_dates
)


def test_eval_formula(tsx):
    tsx.update(
        'test-eval',
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(
                pd.Timestamp('2022-1-1', tz='utc'),
                periods=3,
                freq='d')
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
                freq='d')
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

    ts = tsx.eval_formula(
        '(+ 1 (series "test-eval"))',
        tz='CET'
    )
    assert_df("""
2022-01-01 01:00:00+01:00    2.0
2022-01-02 01:00:00+01:00    3.0
2022-01-03 01:00:00+01:00    4.0
2022-01-04 01:00:00+01:00    5.0
2022-01-05 01:00:00+01:00    6.0
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
    assert err.value.args[0] == "'1' not a Number"


def test_eval_formula_naive(tsx):
    tsx.update(
        'test-eval-naive',
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(
                pd.Timestamp('2022-1-1'),
                periods=3,
                freq='d')
        ),
        'Babar',
        insertion_date=pd.Timestamp('2022-1-5', tz='utc')
    )

    ts = tsx.eval_formula(
        '(+ 1 (series "test-eval-naive"))',
        tz='CET'
    )
    assert_df("""
2022-01-01    2.0
2022-01-02    3.0
2022-01-03    4.0
""", ts)


def test_bogus_formula(tsx):
    with pytest.raises(TypeError) as err:
        tsx.register_formula(
            'bogus',
            '(resample "nope")'
        )
    print(err)


def test_local_formula_remote_series(tsa):
    rtsh = timeseries('remote')
    rtsh.update(
        tsa.engine,
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(pd.Timestamp('2020-1-1'), periods=3, freq='h'),
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

    expanded = tsa.formula('remote-formula-local-formula', expanded=True, display=False)
    assert expanded == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (+ 3 (+ 2 (series "remote-series")))'
        ')'
    )
    expanded = tsa.formula(
        'remote-formula-local-formula', expanded=True
    )
    assert expanded == (
        '(+ 3 (+ 2 (series "remote-series")))'
    )


def test_formula_remote_expansion(tsa):
    rtsh = timeseries('remote')
    rtsh.update(
        tsa.engine,
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(pd.Timestamp('2024-1-1'), periods=3, freq='h'),
        ),
        'remote-base-series',
        'Babar',
        insertion_date=pd.Timestamp('2024-1-1', tz='UTC')
    )
    rtsh.register_formula(
        tsa.engine,
        'remote-formula',
        '(+ 1 (series "remote-base-series"))'
    )

    tsa.register_formula(
        'test-localformula-remote-expansion',
        '(+ 1 (series "remote-formula"))'
    )

    f = tsa.formula('test-localformula-remote-expansion')
    assert f == '(+ 1 (series "remote-formula"))'
    d = tsa.formula_depth('test-localformula-remote-expansion')
    assert d == 1

    f = tsa.formula('test-localformula-remote-expansion', expanded=True, display=True, remote=False)
    # the remote formula was not expanded
    assert f == '(+ 1 (series "remote-formula"))'

    f = tsa.formula('test-localformula-remote-expansion', expanded=True, display=True)
    assert f == '(+ 1 (+ 1 (series "remote-base-series")))'


def test_formula_remote_double_expansion(tsa):
    rtsh = timeseries('remote')
    rtsh.update(
        tsa.engine,
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(pd.Timestamp('2024-1-1'), periods=3, freq='h'),
        ),
        'remote-primary-series',
        'Babar',
        insertion_date=pd.Timestamp('2024-1-1', tz='UTC')
    )
    rtsh.register_formula(
        tsa.engine,
        'remote-formula-2',
        '(+ 2 (series "remote-primary-series"))'
    )
    rtsh.register_formula(
        tsa.engine,
        'remote-formula-1',
        '(+ 1 (series "remote-formula-2"))'
    )

    tsa.register_formula(
        'test-remote-formula-double-expansion',
        '(+ 1 (series "remote-formula-1"))'
    )

    f = tsa.formula('test-remote-formula-double-expansion')
    assert f == '(+ 1 (series "remote-formula-1"))'
    d = tsa.formula_depth('test-remote-formula-double-expansion')
    assert d == 2

    f = tsa.formula(
        'test-remote-formula-double-expansion',
        expanded=True, display=True, remote=False
    )
    # the remote formula was not expanded
    assert f == '(+ 1 (series "remote-formula-1"))'

    # try levels
    f = tsa.formula(
        'test-remote-formula-double-expansion',
        display=True, level=0
    )
    assert f == '(+ 1 (series "remote-formula-1"))'

    f = tsa.formula(
        'test-remote-formula-double-expansion',
        expanded=True, display=True, level=1
    )
    assert f == '(+ 1 (+ 1 (series "remote-formula-2")))'

    f = tsa.formula(
        'test-remote-formula-double-expansion',
        expanded=True, display=True, level=2
    )
    assert f == '(+ 1 (+ 1 (+ 2 (series "remote-primary-series"))))'


def test_findseries_and_remote_byname(tsa, engine):
    ts = pd.Series(
        [1, 2, 3],
        pd.date_range(pd.Timestamp('2024-1-1', tz='UTC'), freq='D', periods=3)
    )
    rtsh = timeseries('remote')
    rtsh.update(
        engine,
        ts,
        'I am remote, can you find me ?',
        'Babar'
    )

    tsa.register_formula(
        'findremote',
        '(add (findseries (by.and (by.name "I am") (by.name "find me ?"))))'
    )
    ts = tsa.get('findremote')
    assert len(ts) == 3


def test_oldformulas(tsx):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='d')
    )

    tsx.update(
        'form-hist-base', ts, 'Babar'
    )

    tsx.register_formula(
        'form-hist',
        '(add (series "form-hist-base") (series "form-hist-base"))'
    )
    # no-op
    tsx.register_formula(
        'form-hist',
        '(add (series "form-hist-base") (series "form-hist-base"))'
    )
    assert len(tsx.oldformulas('form-hist')) == 0

    tsx.register_formula(
        'form-hist',
        '(* 2 (series "form-hist-base"))'
    )
    tsx.register_formula(
        'form-hist',
        '(+ .1 (* 2 (series "form-hist-base")))'
    )

    hist = tsx.oldformulas('form-hist')
    assert hist[-1][0] == '(add (series "form-hist-base") (series "form-hist-base"))'
    assert hist[0][0] == '(* 2 (series "form-hist-base"))'
    assert hist[0][2] == 'no-user'

    tsx.delete('form-hist')
    assert tsx.oldformulas('form-hist') == []


def test_remote_oldformulas(tsa):
    rtsh = timeseries('remote')
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='d')
    )
    engine = tsa.engine
    rtsh.update(
        engine,
        ts,
        'form-hist-base', 'Babar'
    )

    rtsh.register_formula(
        engine,
        'form-hist',
        '(add (series "form-hist-base") (series "form-hist-base"))'
    )
    rtsh.register_formula(
        engine,
        'form-hist',
        '(* 2 (series "form-hist-base"))'
    )

    hist = tsa.oldformulas('form-hist')
    assert len(hist) == 1



def test_formula_components(tsa):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2020-6-1'), freq='d', periods=3)
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
    rtsh = timeseries('remote')
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


def test_formula_components_findseries(tsa):
    series = pd.Series(
        [1, 2, 3],
        index=pd.date_range(pd.Timestamp('2023-1-1', tz='UTC'), freq='d', periods=3)
    )
    tsa.update(
        'comp-findseries-a',
        series,
        'Babar'
    )
    tsa.update(
        'comp-findseries-b',
        series,
        'Celeste'
    )

    form = (
        '(priority '
        '   (add (findseries (by.name "comp-findseries")))'
        '   (series "comp-findseries-b"))'
    )
    tsa.register_formula(
        'show-dyn-comp',
        form
    )

    components = tsa.formula_components('show-dyn-comp')

    # let's teach .formula_components to deal with `findseries` !
    assert components == {
        'show-dyn-comp': ['comp-findseries-a', 'comp-findseries-b']
    }


def test_rename(tsa):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(datetime(2019, 1, 1), periods=3, freq='d')
    )
    tsa.update('rename-a', ts, 'Babar')

    tsa.register_formula(
        'survive-renaming',
        '(+ 1 (series "rename-a" #:fill 0))'
    )
    tsa.register_formula(
        'survive-renaming-2',
        '(add (series "survive-renaming") (series "rename-a" #:fill 0))'
    )

    ts = tsa.get('survive-renaming')
    assert_df("""
2019-01-01    2.0
2019-01-02    3.0
2019-01-03    4.0
""", ts)

    ts = tsa.get('survive-renaming-2')
    assert_df("""
2019-01-01    3.0
2019-01-02    5.0
2019-01-03    7.0
""", ts)

    with pytest.raises(Exception):
        tsa.rename('rename-a', ' ')

    tsa.rename('rename-a', 'a-renamed')

    ts = tsa.get('survive-renaming')
    assert_df("""
2019-01-01    2.0
2019-01-02    3.0
2019-01-03    4.0
""", ts)

    ts = tsa.get('survive-renaming-2')
    assert_df("""
2019-01-01    3.0
2019-01-02    5.0
2019-01-03    7.0
""", ts)

    with pytest.raises(ValueError) as err:
        tsa.rename('a-renamed', 'survive-renaming')

    assert err.value.args[0] == 'new name is already referenced by `survive-renaming-2`'

    # rename a formula !
    tsa.rename('survive-renaming', 'survived')
    assert tsa.formula(
        'survive-renaming-2'
    ) == '(add (series "survived") (series "a-renamed" #:fill 0))'

    assert tsa.formula(
        'survived'
    ) == '(+ 1 (series "a-renamed" #:fill 0))'

    # propagate option
    tsa.rename('a-renamed', 'b-renamed', propagate=False)

    assert tsa.formula(
        'survive-renaming-2'
    ) == '(add (series "survived") (series "a-renamed" #:fill 0))'

    ts = pd.Series(
        [4, 5, 6],
        index=pd.date_range(datetime(2019, 1, 1), periods=3, freq='d')
    )
    tsa.update('a-renamed', ts, 'Babar')

    ts = tsa.get('survive-renaming-2')
    assert_df("""
2019-01-01     9.0
2019-01-02    11.0
2019-01-03    13.0
""", ts)


def test_formula_depth(tsx):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2022, 1, 1), periods=3, freq='d')
    )
    tsx.update('level-base', ts, 'Babar')
    tsx.register_formula('level-0', '(+ 1 (series "level-base"))')
    tsx.register_formula('level-1', '(+ 1 (series "level-0"))')
    tsx.register_formula('level-2', '(+ 1 (series "level-1"))')

    assert tsx.formula_depth('level-0') == 0
    assert tsx.formula_depth('level-1') == 1
    assert tsx.formula_depth('level-2') == 2

    exp = tsx.formula('level-2', expanded=True, level=0, display=False)
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil '
        '(+ 1 (series "level-1"))'
        ')'
    )
    exp = tsx.formula('level-2', expanded=True, level=1, display=False)
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil '
        '(+ 1 (+ 1 (series "level-0")))'
        ')'
    )
    exp = tsx.formula('level-2', expanded=True, level=2, display=False)
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil '
        '(+ 1 (+ 1 (+ 1 (series "level-base"))))'
        ')'
    )
    exp3 = tsx.formula('level-2', expanded=True, level=3, display=False)
    assert exp3 == exp


def test_formula_remote_autotrophic(tsa, engine):
    from tshistory_formula.registry import func, metadata
    from tshistory_formula.tsio import timeseries as pgseries

    @func('customseries')
    def customseries() -> pd.Series:
        return pd.Series(
            [1.0, 2.0, 3.0],
            index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='d')
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

    rtsh = pgseries('remote')
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
        index=pd.date_range(pd.Timestamp('2020-6-1'), freq='d', periods=3)
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
    def custom_finder(cn, tsh, tree):
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
            pd.date_range(utcdt(2020, 1, 1), periods=1, freq='d')
        )

    @finder('autotrophic')
    def custom_finder(cn, tsh, tree):
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
            pd.date_range(utcdt(2020, 1, 1), periods=1, freq='d')
        )

    @finder('auto2')
    def custom_finder(cn, tsh, tree):
        return {
            'I HAVE A NAME FOR DISPLAY PURPOSES': tree
        }

    @insertion_dates('auto2')
    def custom_idates(cn, tsh, tree,
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


def test_history_with_spurious_keys(tsx):
    """
    in certain case, the formula can produce histories
    with keys that index empty series
    """
    i0 = utcdt(2023, 3, 1)
    i1 = utcdt(2023, 3, 2)
    lb = datetime(2023, 3, 1)
    ub = datetime(2023, 3, 4)
    ts = pd.Series(
        range(4),
        index=pd.date_range(
            start=lb,
            end=ub,
            periods=4
        )
    )

    # at date i0, only series-x is defined
    tsx.update('series-x', ts, 'test', insertion_date=i0)
    tsx.update('series-y', ts, 'test', insertion_date=i1)

    tsx.register_formula(
       'simple-addition',
        '(add (series "series-x") (series "series-y"))'
    )

    hist = tsx.history('simple-addition')
    assert len(hist) == 2
    first_key = list(hist.keys())[0]

    # the first key index an empty series
    # it should be removed
    assert len(hist[first_key]) == 0



# groups

def test_group_formula(tsa):
    df = gengroup(
        n_scenarios=3,
        from_date=datetime(2015, 1, 1),
        length=5,
        freq='d',
        seed=2
    )

    df.columns = ['a', 'b', 'c']

    tsa.group_replace('groupa', df, 'test')

    plain_ts = pd.Series(
        [1] * 7,
        index=pd.date_range(
            start=datetime(2014, 12, 31),
            freq='d',
            periods=7,
        )
    )

    tsa.update('plain_tsa', plain_ts, 'Babar')

    # start to test

    formula = (
        '(group-add-series '
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
    assert tsa.group_internal_metadata('difference') == {
        'tzaware': False,
        'formula': '(group-add-series (group "groupa") (* -1 (series "plain_tsa")))',
        'index_type': 'datetime64[ns]',
        'value_type': 'float64',
        'index_dtype': '<M8[ns]',
        'value_dtype': '<f8'
    }

    # formula of formula
    # we add the same series that was substracted,
    # hence we must retrieve the original dataframe group1
    formula = (
        '(group-add-series '
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

    tsa.group_rename('difference', 'difference2')
    tsa.group_rename('groupa', 'groupb')

    gf = tsa.group_formula('roundtripeda')
    assert gf == '(group-add-series (group "difference2") (series "plain_tsa"))'

    df_roundtrip = tsa.group_get('roundtripeda')
    df_original = tsa.group_get('groupb')
    assert df_roundtrip.equals(df_original)


def test_no_group(tsa):
    tsa.group_get('nope')
    # didn't crash ;)


def test_group_eval_formula(tsx):
    df = gengroup(
        n_scenarios=3,
        from_date=datetime(2025, 5, 1),
        length=5,
        freq='D',
        seed=2
    )

    df.columns = ['a', 'b', 'c']

    tsx.group_update('group1', df, 'test')

    df = tsx.group_eval_formula(
        '(group-add (group "group1") (group "group1"))',
        from_value_date=pd.Timestamp('2025-5-2', tz='utc'),
        to_value_date=pd.Timestamp('2025-5-10', tz='utc')
    )
    assert_df("""
               a     b     c
2025-05-02   6.0   8.0  10.0
2025-05-03   8.0  10.0  12.0
2025-05-04  10.0  12.0  14.0
2025-05-05  12.0  14.0  16.0
""", df)

    #naive
    df = tsx.group_eval_formula(
        '(group-add (group "group1") (group "group1"))',
        tz='CET'
    )
    assert_df("""
               a     b     c
2025-05-01   4.0   6.0   8.0
2025-05-02   6.0   8.0  10.0
2025-05-03   8.0  10.0  12.0
2025-05-04  10.0  12.0  14.0
2025-05-05  12.0  14.0  16.0
""", df)

    #tzaware
    df = gengroup(
        n_scenarios=3,
        from_date=pd.Timestamp('2025-5-1', tz='utc'),
        length=5,
        freq='h',
        seed=2
    )

    df.columns = ['a', 'b', 'c']

    tsx.group_update('group-hourly', df, 'test')

    df = tsx.group_eval_formula(
        '(group-add (group "group-hourly") (group "group-hourly"))',
        tz='CET'
    )
    assert_df("""
                              a     b     c
2025-05-01 02:00:00+02:00   4.0   6.0   8.0
2025-05-01 03:00:00+02:00   6.0   8.0  10.0
2025-05-01 04:00:00+02:00   8.0  10.0  12.0
2025-05-01 05:00:00+02:00  10.0  12.0  14.0
2025-05-01 06:00:00+02:00  12.0  14.0  16.0
""", df)

    #bogus
    with pytest.raises(SyntaxError):
        tsx.group_eval_formula(
            '(group-add (")'
        )

    with pytest.raises(TypeError) as err:
        tsx.group_eval_formula(
            '(group-add (fake-operator "test-eval") (group "kikou"))'
        )
    assert err.value.args[0] == (
        'expression `(fake-operator "test-eval")` '
        'refers to unknown operator `fake-operator`'
    )

def test_group_bound_formula(tsa):
    temp = pd.Series(
        [12, 13, 14],
        index=pd.date_range(utcdt(2021, 1, 1), freq='d', periods=3)
    )
    wind = pd.Series(
        [.1, .1, .1],
        index=pd.date_range(utcdt(2021, 1, 1), freq='d', periods=3)
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
        freq='d',
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
        freq='d',
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

    # second bound formula with the same recipe succeeds
    tsa.register_formula_bindings(
        'hijacking3',
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

    # robust to underlying primary group renaming ?
    tsa.group_rename('wind-ens', 'wind-ens2')
    df = tsa.group_get('hijacking')
    assert_df("""
                             0    1
2021-01-01 00:00:00+00:00  1.0  3.0
2021-01-02 00:00:00+00:00  3.0  5.0
2021-01-03 00:00:00+00:00  5.0  7.0
""", df)

    assert tsa.group_exists('hijacking')
    assert tsa.group_type('hijacking') == 'bound'

    res = tsa.group_find('(by.name "hijacking")')
    assert res[0].kind == 'bound'
    res = tsa.group_find('(by.name "hijacking")', meta=True)
    assert res[0].kind == 'bound'

    cat = list(tsa.group_catalog().values())[0]
    assert ('hijacking', 'bound') in cat

    assert tsa.group_metadata('hijacking') == {}
    assert tsa.group_internal_metadata('hijacking') == {
        'bound': True,
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }
    tsa.update_group_metadata('hijacking', {'foo': 'bar'})
    assert tsa.group_metadata('hijacking') == {'foo': 'bar'}
    assert tsa.group_internal_metadata('hijacking') == {
        'bound': True,
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

    # let's rename things
    tsa.rename('base-temp', 'base-temp2')
    tsa.rename('base-wind', 'base-wind2')
    tsa.rename('hijacked', 'hujacked2')
    tsa.group_rename('temp-ens', 'temp-ens2')
    tsa.group_rename('wind-ens', 'wind-ens2')
    tsa.group_rename('hijacking', 'hijacking2')

    assert not tsa.group_exists('hijacking')

    df = tsa.group_get('hijacking2')
    assert_df("""
                             0    1
2021-01-01 00:00:00+00:00  1.0  3.0
2021-01-02 00:00:00+00:00  3.0  5.0
2021-01-03 00:00:00+00:00  5.0  7.0
""", df)

    tsa.group_delete('hijacking2')
    assert not tsa.group_exists('hijacking2')
    assert tsa.group_metadata('hijacking2') is None

    # infos
    info = tsa.info()
    assert 'local' in info
    assert 'remote' in info
    linfo = info['local']
    assert 'bound_groups' in linfo
    assert 'formula_groups' in linfo
    assert 'formula_series' in linfo


def test_more_group_errors(tsx):
    tsx.delete('toto')
    tsx.delete('tata')

    df = gengroup(
        n_scenarios=3,
        from_date=datetime(2015, 1, 1),
        length=5,
        freq='d',
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
        '(constant 42.5 (date "1900-1-1") (date "2039-12-31") (freq "D") (date "1900-1-1"))'
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
                pd.date_range(pd.Timestamp('2021-1-1'), freq='d', periods=3)
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
        '(constant 42.5 (date "1900-1-1") (date "2039-12-31") (freq "D") (date "1900-1-1"))'
    )
    tsx.register_formula_bindings(
        'conflict-name-binding',
        'tata',
        pd.DataFrame([['a', 'b', 'c']], columns=('series', 'group', 'family'))
    )


def test_local_group_formula_remote_group(tsa):
    rtsh = timeseries('remote')
    df = gengroup(
        n_scenarios=3,
        from_date=datetime(2025, 1, 1),
        length=5,
        freq='d',
        seed=2
    )

    rtsh.group_replace(
        tsa.engine,
        df,
        'remote-group',
        'Babar',
        insertion_date=pd.Timestamp('2025-1-1', tz='UTC')
    )

    tsa.register_group_formula(
        'test-localformula-remotegroup',
        '(group "remote-group"))'
    )

    ts = tsa.group_get('test-localformula-remotegroup')
    assert_df("""
              0    1    2
2025-01-01  2.0  3.0  4.0
2025-01-02  3.0  4.0  5.0
2025-01-03  4.0  5.0  6.0
2025-01-04  5.0  6.0  7.0
2025-01-05  6.0  7.0  8.0
""", ts)

    hist = tsa.group_history('test-localformula-remotegroup')
    assert_hist("""
                                        0    1    2
insertion_date            value_date               
2025-01-01 00:00:00+00:00 2025-01-01  2.0  3.0  4.0
                          2025-01-02  3.0  4.0  5.0
                          2025-01-03  4.0  5.0  6.0
                          2025-01-04  5.0  6.0  7.0
                          2025-01-05  6.0  7.0  8.0
""", hist)

    f = tsa.group_formula('test-localformula-remotegroup')
    assert f == '(group "remote-group")'

    none = tsa.group_formula('nosuchformula')
    assert none is None

    # altsource formula
    rtsh.register_group_formula(
        tsa.engine,
        'remote-formula-remote-group',
        '(group "remote-group")'
    )
    f = tsa.group_formula('remote-formula-remote-group')
    assert f == '(group "remote-group")'

    assert_df("""
              0    1    2
2025-01-01  2.0  3.0  4.0
2025-01-02  3.0  4.0  5.0
2025-01-03  4.0  5.0  6.0
2025-01-04  5.0  6.0  7.0
2025-01-05  6.0  7.0  8.0
""", tsa.group_get('remote-formula-remote-group'))

    rtsh.register_group_formula(
        tsa.engine,
        'remote-formula-local-group-formula',
        '(group "remote-formula-remote-group")'
    )
    f = tsa.group_formula('remote-formula-local-group-formula')
    assert f == '(group "remote-formula-remote-group")'

    ts = tsa.group_get('remote-formula-local-group-formula')
    assert_df("""
              0    1    2
2025-01-01  2.0  3.0  4.0
2025-01-02  3.0  4.0  5.0
2025-01-03  4.0  5.0  6.0
2025-01-04  5.0  6.0  7.0
2025-01-05  6.0  7.0  8.0
""", ts)


def test_local_group_formula_remote_bound_group(tsa):
    rtsh = timeseries('remote')
    rtsh.delete(tsa.engine, 'remote-series')
    rtsh.group_delete(tsa.engine, 'remote-group')
    rtsh.group_delete(tsa.engine, 'remote-bound-group')

    rtsh.update(
        tsa.engine,
        pd.Series(
            [1, 2, 3],
            pd.date_range(utcdt(2023, 1, 1), freq='d', periods=3)
        ),
        'remote-series',
        'Babar'
    )

    rtsh.register_formula(
        tsa.engine,
        'remote-formula',
        '(series "remote-series")'
    )

    df = gengroup(
        n_scenarios=3,
        from_date=utcdt(2025, 1, 1),
        length=3,
        freq='d',
        seed=2
    )

    rtsh.group_replace(
        tsa.engine,
        df,
        'remote-group',
        'Celeste'
    )

    rtsh.register_formula_bindings(
        tsa.engine,
        'remote-bound-group',
        'remote-formula',
        pd.DataFrame(
            [
                ['remote-series', 'remote-group', 'family'],
            ],
            columns=('series', 'group', 'family')
        )
    )

    # now the local part
    tsa.register_group_formula(
        'local-group-remote-bound-group',
        '(group "remote-bound-group")'
    )
    df = tsa.group_get('local-group-remote-bound-group')
    assert_df("""
                             0    1    2
2025-01-01 00:00:00+00:00  2.0  3.0  4.0
2025-01-02 00:00:00+00:00  3.0  4.0  5.0
2025-01-03 00:00:00+00:00  4.0  5.0  6.0
""", df)

    # ask for the bindings
    boundseries, binding = tsa.bindings_for('remote-bound-group')
    assert boundseries == 'remote-formula'
    assert binding.to_dict() == {
        'series': {0: 'remote-series'},
        'group': {0: 'remote-group'},
        'family': {0: 'family'}
    }


def test_group_find(tsx):
    df = gengroup(
        n_scenarios=3,
        from_date=datetime(2025, 1, 1),
        length=2,
        freq='d',
        seed=2
    )

    tsx.group_replace('basegroup', df, 'test')
    formula = (
        '(group-add '
        '  (group "basegroup") '
        '  (group "basegroup")) '
    )

    tsx.register_group_formula(
        '2groups',
        formula
    )

    res = tsx.group_find('(by.name "basegroup")')
    assert res[0].kind == 'primary'

    res = tsx.group_find('(by.name "2groups")')
    assert res[0].kind == 'formula'


def test_find(tsx):
    ts = pd.Series(
        [1, 2, 3],
        pd.date_range(utcdt(2023, 1, 1), freq='d', periods=3)
    )
    tsx.update(
        'base.find',
        ts,
        'Babar'
    )

    tsx.register_formula(
        'find.bycontent.add',
        '(add (series "base.find") (series "base.find"))'
    )

    tsx.register_formula(
        'find.bycontent.integration',
        '(integration "base.find" "base.find")'
    )

    assert tsx.find('(by.name "base.find")')[0].kind == 'primary'
    assert tsx.find('(by.name "base.find")', meta=True)[0].kind == 'primary'

    names = tsx.find('(by.formulacontents "integration")')
    assert names == ['find.bycontent.integration']

    names = tsx.find('(by.formulacontents "add")')
    assert names == ['find.bycontent.add']
    assert names[0].kind == 'formula'
    names = tsx.find('(by.formulacontents "add")', meta=True)
    assert names[0].kind == 'formula'

    names = tsx.find('(by.formula)')
    assert names == [
        'find.bycontent.add',
        'find.bycontent.integration'
    ]

    names = tsx.find('(by.not (by.formula))')
    assert names == ['base.find']

    # basket
    tsx.register_basket('integration', '(by.formulacontents "integration")')
    names = tsx.basket('integration')
    assert names == ['find.bycontent.integration']


def test_depends(tsx):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2022, 1, 1), periods=3, freq='d')
    )

    tsx.update(
        'depends-base',
        ts,
        'Babar'
    )

    tsx.register_formula(
        'depends-bottom',
        '(series "depends-base")'
    )
    tsx.register_formula(
        'depends-middle-left',
        '(+ -1 (series "depends-bottom"))'
    )
    tsx.register_formula(
        'depends-middle-right',
        '(+ 1 (series "depends-bottom"))'
    )
    tsx.register_formula(
        'depends-top',
        '(add (series "depends-middle-left") (series "depends-middle-right"))'
    )
    assert tsx.depends('depends-top', reverse=True) == []
    assert tsx.depends('depends-top', direct=True) == [
        'depends-middle-left',
        'depends-middle-right'
    ]
    assert tsx.depends('depends-top') == [
        'depends-base',
        'depends-bottom',
        'depends-middle-left',
        'depends-middle-right'
    ]

    assert tsx.depends('depends-middle-left', reverse=True) == [
        'depends-top',
    ]
    assert tsx.depends('depends-middle-left', direct=True) == ['depends-bottom']
    assert tsx.depends('depends-middle-left') == [
        'depends-base',
        'depends-bottom'
    ]

    assert tsx.depends('depends-middle-right', reverse=True) == [
        'depends-top',
    ]
    assert tsx.depends('depends-middle-right', direct=True) == ['depends-bottom']
    assert tsx.depends('depends-middle-right') == ['depends-base', 'depends-bottom']

    assert tsx.depends('depends-bottom', reverse=True) == [
        'depends-middle-left',
        'depends-middle-right',
        'depends-top'
    ]
    assert tsx.depends('depends-bottom', direct=True) == ['depends-base']
    assert tsx.depends('depends-bottom') == ['depends-base']

    assert tsx.depends('depends-bottom', reverse=True, direct=True) == [
        'depends-middle-left',
        'depends-middle-right'
    ]

    # update and see

    tsx.register_formula(
        'depends-bottom-2',  # an alternative to bottom
        '(series "depends-base")'
    )

    tsx.register_formula(
        'depends-middle-left',
        '(+ -1 (series "depends-bottom-2"))'
    )
    tsx.register_formula(
        'depends-top',
        '(add'
        ' (series "depends-middle-right")'
        ' (series "depends-middle-right"))'
    )
    assert tsx.depends('depends-bottom-2', reverse=True) == [
        'depends-middle-left',
    ]
    assert tsx.depends('depends-bottom', reverse=True) == [
        'depends-middle-right',
        'depends-top'
    ]

    # delete things and see
    tsx.delete('depends-top')

    assert tsx.depends('depends-bottom', reverse=True) == [
        'depends-middle-right'
    ]

    tsx.delete('depends-middle-right')
    assert tsx.depends('depends-middle-left', reverse=True) == []
    assert tsx.depends('depends-middle-right', reverse=True) == []


def test_depends_auto(tsx):
    tsx.register_formula(
        'depends-fbase',
        '(constant 1. (date "2025-1-1") (date "2025-1-3") (freq "D") (date "2025-2-1"))'
    )

    tsx.register_formula(
        'use-auto',
        '(series "depends-fbase")'
    )

    assert tsx.depends('use-auto') == ['depends-fbase']


# tree

def test_tree_api(tsx, engine):
    tsx.set_tree_attribute(None)
    assert tsx.tree_attribute() is None
    tsx.set_tree_attribute('folder')
    assert tsx.tree_attribute() == 'folder'

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2020, 1, 1), freq='d', periods=3)
    )

    for name in (
            'UE.Italy',
            'UE.France'
    ):
        sname = name.lower()
        tsx.update(
            sname,
            ts,
            'Babar'
        )
        tsx.update_metadata(sname, {'folder': name})

    assert tsx.path_series('UE.France') == ['ue.france']
    assert tsx.path_series('UE.Italy') == ['ue.italy']
    assert tsx.path_series('UE') == []

    assert tsx.series_path('ue.france') == 'UE.France'
    assert tsx.series_path('ue.italy') == 'UE.Italy'

    assert tsx.tree() == ['UE.Italy', 'UE.France']

    tsx.delete_path('UE.Italy')
    assert tsx.tree() == ['UE.France']

    sl = tsx.find('(by.metaitem "folder" "UE.Italy")')
    assert sl == ['ue.italy']

    assert tsx.path_series('UE.France') == ['ue.france']
    assert tsx.path_series('UE.Italy') == []

    assert tsx.series_path('ue.france') == 'UE.France'
    assert tsx.series_path('ue.italy') is None
