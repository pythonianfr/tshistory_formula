from datetime import datetime as dt, timedelta

from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import pytest

from psyl import lisp
from tshistory.testutil import (
    assert_df,
    assert_hist,
    gengroup,
    utcdt
)

from tshistory_formula.decorator import decorate
from tshistory_formula.registry import (
    func,
    FUNCS,
    finder,
    HISTORY,
    history,
    metadata,
    gfunc,
    gfinder,
    ginsertion_dates,
    gmeta
)
from tshistory_formula.types import constant_fold
from tshistory_formula.helper import (
    _extract_from_expr,
    expanded,
    has_names,
    _name_from_signature_and_args,
    name_of_expr,
    rename_operator,
    find_autos,
    rewrite_trig_formula,
    scan_descendant_nodes,
)
from tshistory_formula.interpreter import (
    Interpreter,
    NullIntepreter,
    OperatorHistory,
    GroupInterpreter,
)


def test_evaluator():
    form = '(+ 2 3)'
    with pytest.raises(LookupError):
        e = lisp.evaluate(form, lisp.Env())

    env = lisp.Env({'+': lambda a, b: a + b})
    e = lisp.evaluate(form, env)
    assert e == 5

    brokenform = '(+ 2 3'
    with pytest.raises(SyntaxError):
        lisp.parse(brokenform)

    expr = ('(+ (* 8 (/ 5. 2)) 1.1)')
    tree = constant_fold(lisp.parse(expr))
    assert tree == 21.1

    expr = ('(+ (* 8 (/ 5. 2)) (series "foo"))')
    tree = constant_fold(lisp.parse(expr))
    assert tree == ['+', 20.0, ['series', 'foo']]


def test_bad_toplevel_type(engine, tsh):
    msg = 'formula `test_bad_toplevel_type` must return a `Series`, not `int`'
    with pytest.raises(TypeError, match=msg):
        tsh.register_formula(
            engine,
            'test_bad_toplevel_type',
            '(+ 2 (* 3 4))',
        )

    msg = 'formula `test_bad_toplevel_type` must return a `Series`, not `float`'
    with pytest.raises(TypeError, match=msg):
        tsh.register_formula(
            engine,
            'test_bad_toplevel_type',
            '(+ 2 (* 3 (/ 8 4)))',
        )


def test_rename_operator():
    form = '(foo 1 (bar 5 (foo 6)))'
    tree = lisp.parse(form)
    assert lisp.serialize(
        rename_operator(tree, 'foo', 'FOO')
    ) == '(FOO 1 (bar 5 (FOO 6)))'


def test_bad_name(engine, tsh):
    with pytest.raises(AssertionError):
        tsh.register_formula(
            engine,
            ' ',
            '(series "foo")'
        )


def test_finder(engine, tsh):
    naive = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )

    tsh.update(engine, naive, 'finder', 'Babar',
               insertion_date=utcdt(2019, 1, 1))

    tsh.register_formula(
        engine,
        'test_finder',
        '(+ 2 (series "finder"))',
    )

    parsed = lisp.parse(
        tsh.formula(
            engine, 'test_finder'
        )
    )
    found = tsh.find_series(engine, parsed)
    assert found == {
        'finder': parsed[2]
    }

    tsh.register_formula(
        engine,
        'test_finder_primary_plus_formula',
        '(add (series "test_finder") (series "finder"))',
    )
    parsed = lisp.parse(
        tsh.formula(
            engine, 'test_finder_primary_plus_formula'
        )
    )
    found = tsh.find_series(engine, parsed)
    assert found == {
        'finder': parsed[2],
        'test_finder': parsed[1]
    }


def test_metadata(engine, tsh):
    naive = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )

    tsh.update(engine, naive, 'metadata_naive', 'Babar',
               insertion_date=utcdt(2019, 1, 1))

    tsh.register_formula(
        engine,
        'test_meta',
        '(+ 2 (series "metadata_naive"))',
    )

    assert tsh.internal_metadata(engine, 'test_meta') == {
        'contenthash': '3255418caaa64aaec854e9b2fcabe9f0ee95d866',
        'formula': '(+ 2 (series "metadata_naive"))',
        'index_dtype': '<M8[ns]',
        'index_type': 'datetime64[ns]',
        'tzaware': False,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

    aware = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, aware, 'metadata_tzaware', 'Babar',
               insertion_date=utcdt(2019, 1, 1))


    with pytest.raises(ValueError) as err:
        tsh.register_formula(
            engine,
            'test_meta_mismatch',
            '(add (series "test_meta") (series "metadata_tzaware"))',
        )
    assert err.value.args[0] == (
        "Formula `metadata_tzaware` has tzaware vs tznaive series:`"
        "('test_meta', ('add, 'series)):tznaive`,"
        "`('metadata_tzaware', ('add, 'series)):tzaware`"
    )

    tsh.register_formula(
        engine,
        'test_meta_primary_plus_formula',
        '(add (series "test_meta") (series "metadata_naive"))',
    )
    meta = tsh.internal_metadata(engine, 'test_meta_primary_plus_formula')
    assert meta == {
        'contenthash': 'c10807ce39ace1c56c0bc53ff77e2981aa64c2ec',
        'formula': '(add (series "test_meta") (series "metadata_naive"))',
        'index_dtype': '<M8[ns]',
        'index_type': 'datetime64[ns]',
        'tzaware': False,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }


def test_user_meta(engine, tsh):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )

    tsh.update(engine, ts, 'user_metadata', 'Babar',
               insertion_date=utcdt(2021, 1, 1))

    tsh.register_formula(
        engine,
        'test_user_meta',
        '(+ 2 (series "user_metadata"))',
    )

    tsh.update_metadata(
        engine,
        'test_user_meta',
        {'foo': 42}
    )

    meta = tsh.metadata(engine, 'test_user_meta')
    assert meta['foo'] == 42
    assert meta == {
        'foo': 42
    }

    tsh.register_formula(
        engine,
        'test_user_meta',
        '(+ 3 (naive (series "user_metadata")))'
    )
    meta = tsh.metadata(engine, 'test_user_meta')
    # user meta preserved, core meta correctly updated
    assert meta == {
        'foo': 42
    }


def test_first_latest_insertion_date(engine, tsh):
    for i in range(3):
        ts = pd.Series(
            [i] * 3,
            index=pd.date_range(
                utcdt(2022, 1, i+1),
                freq='D',
                periods=3
            )
        )
        tsh.update(
            engine,
            ts,
            'test-f-l-idate',
            'Babar',
            insertion_date=utcdt(2022, 1, i+1)
        )

    name = 'idate-f-l'
    tsh.register_formula(
        engine,
        name,
        '(series "test-f-l-idate")'
    )

    idates = tsh.insertion_dates(engine, name)
    assert tsh.first_insertion_date(engine, name) == idates[0]
    assert tsh.latest_insertion_date(engine, name) == idates[-1]


def test_series_options(engine, tsh):
    test = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, test, 'options-a', 'Babar')
    tsh.update(engine, test, 'options-b', 'Babar')
    tsh.register_formula(
        engine,
        'test_series_option',
        '(add (series "options-a") (series "options-b"))',
    )

    ts = tsh.get(engine, 'test_series_option')
    assert ts.options == {}


def test_fill_limit_option(engine, tsh):
    shortts = pd.Series(
        [1, 1, 1],
        index=pd.date_range(dt(2022, 1, 1), periods=3, freq='D')
    )
    longts = pd.Series(
        [2, 2, 2, 2, 2],
        index=pd.date_range(dt(2022, 1, 1), periods=5, freq='D')
    )
    tsh.update(engine, shortts, 'short-ts', 'Babar')
    tsh.update(engine, longts, 'long-ts', 'Babar')
    tsh.register_formula(
        engine,
        'fill-limit-base',
        '(add (series "short-ts" #:fill 7 #:limit 1)'
        '     (series "long-ts"))'
    )
    ts = tsh.get(engine, 'fill-limit-base')
    assert_df("""
2022-01-01    3.0
2022-01-02    3.0
2022-01-03    3.0
2022-01-04    9.0""", ts)


def test_override_primary(engine, tsh):
    test = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, test, 'a-primary', 'Babar')

    with pytest.raises(TypeError) as err:
        tsh.register_formula(
            engine,
            'a-primary',
            '(+ 3 (series "a-primary"))'
        )

    assert err.value.args[0] == (
        'primary series `a-primary` cannot be overriden by a formula'
    )


def _test_override_formula(engine, tsh):
    test = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, test, 'a-primary', 'Babar')

    tsh.register_formula(
        engine,
        'override-me',
        '(* 2 (series "a-primary"))'
    )

    with pytest.raises(ValueError):
        tsh.update(engine, test, 'override-me', 'Babar')


def test_normalization(engine, tsh):
    test = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, test, 'normalize', 'Babar')
    tsh.register_formula(
        engine,
        'test_normalization',
        '( add ( series "normalize") ( series  "normalize" )\n  ) ',
    )

    form = tsh.formula(engine, 'test_normalization')
    assert form == '(add (series "normalize") (series "normalize"))'


def test_content_hash(engine, tsh):
    tsh.register_formula(engine, 'hash-me', '(+ 2 (series "test"))', False)
    ch = tsh.content_hash(engine, 'hash-me')
    assert ch == '9f0b50a52e5895f580c7cca75a907267b401cb6a'

    # identical
    tsh.register_formula(engine, 'hash-me', '(+ 2 (series "test"))', False)
    ch = tsh.content_hash(engine, 'hash-me')
    assert ch == '9f0b50a52e5895f580c7cca75a907267b401cb6a'
    assert ch == tsh.live_content_hash(engine, 'hash-me')

    tsh.register_formula(engine, 'hash-me', '(+ 2 (series "test-2"))', False)
    ch = tsh.content_hash(engine, 'hash-me')
    assert ch == '8be58ac4b7ff2f72b68eadca8f98bd7533eca1ba'
    assert ch == tsh.live_content_hash(engine, 'hash-me')


def test_base_api(engine, tsh):
    tsh.register_formula(engine, 'test_plus_two', '(+ 2 (series "test"))', False)
    tsh.register_formula(engine, 'test_three_plus', '(+ 3 (series "test"))', False)

    # accept an update
    tsh.register_formula(engine, 'test_plus_two', '(+ 2 (series "test"))',
                         reject_unknown=False)

    test = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )

    tsh.update(engine, test, 'test', 'Babar',
               insertion_date=utcdt(2019, 1, 1))

    twomore = tsh.get(engine, 'test_plus_two')
    assert_df("""
2019-01-01    3.0
2019-01-02    4.0
2019-01-03    5.0
""", twomore)

    nope = tsh.get(engine, 'test_plus_two', revision_date=utcdt(2018, 1, 1))
    assert len(nope) == 0

    evenmore = tsh.get(engine, 'test_three_plus')
    assert_df("""
2019-01-01    4.0
2019-01-02    5.0
2019-01-03    6.0
""", evenmore)

    tsh.register_formula(engine, 'test_product_a', '(* 1.5 (series "test"))', False)
    tsh.register_formula(engine, 'test_product_b', '(* 2 (series "test"))', False)

    series = tsh.list_series(engine)
    assert series['test'] == 'primary'
    assert series['test_product_a'] == 'formula'

    plus = tsh.get(engine, 'test_product_a')
    assert_df("""
2019-01-01    1.5
2019-01-02    3.0
2019-01-03    4.5
""", plus)

    plus = tsh.get(engine, 'test_product_b')
    assert_df("""
2019-01-01    2.0
2019-01-02    4.0
2019-01-03    6.0
""", plus)

    m = tsh.internal_metadata(engine, 'test_product_a')
    assert m == {
        'contenthash': '6f43a6a9181beacdea858482ed30a05ea5247401',
        'formula': '(* 1.5 (series "test"))',
        'index_dtype': '<M8[ns]',
        'index_type': 'datetime64[ns]',
        'tzaware': False,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }
    m = tsh.metadata(engine, 'test_product_a')
    assert m == {}

    tsh.update_metadata(engine, 'test_product_a', {'topic': 'spot price'})
    m = tsh.metadata(engine, 'test_product_a')
    assert m == {
        'topic': 'spot price'
    }

    tsh.update_metadata(
        engine, 'test_product_a', {
            'topic': 'Spot Price',
            'unit': '€'
        }
    )
    m = tsh.metadata(engine, 'test_product_a')
    assert m == {
        'topic': 'Spot Price',
        'unit': '€',
    }

    tsh.update_metadata(
        engine, 'test_product_a', {
            'unit': '€'
        }
    )
    m = tsh.metadata(engine, 'test_product_a')
    assert m == {
        'unit': '€',
    }

    tsh.delete(engine, 'test_plus_two')
    assert not tsh.exists(engine, 'test_plus_two')


def test_boolean_support(engine, tsh):
    @func('op-with-boolean-kw')
    def customseries(zeroes: bool=False) -> pd.Series:
        return pd.Series(
            np.array([1.0, 2.0, 3.0]) * zeroes,
            index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
        )

    tsh.register_formula(
        engine,
        'no-zeroes',
        '(op-with-boolean-kw)'
    )
    tsh.register_formula(
        engine,
        'zeroes',
        '(op-with-boolean-kw #:zeroes #t)'
    )

    ts1 = tsh.get(engine, 'no-zeroes')
    assert_df("""
2019-01-01    0.0
2019-01-02    0.0
2019-01-03    0.0
""", ts1)

    ts2 = tsh.get(engine, 'zeroes')
    assert_df("""
2019-01-01    1.0
2019-01-02    2.0
2019-01-03    3.0
""", ts2)

    FUNCS.pop('op-with-boolean-kw')


def test_scalar_ops(engine, tsh):
    x = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2020, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, x, 'scalar-ops', 'Babar')

    tsh.register_formula(
        engine,
        'scalar-formula',
        '(+ (+ (/ 20 (* 2 5)) 1) (series "scalar-ops"))',
    )
    ts = tsh.get(engine, 'scalar-formula')
    assert_df("""
2020-01-01    4.0
2020-01-02    5.0
2020-01-03    6.0
""", ts)


def test_options(engine, tsh):
    @func('dummy')
    def dummy(option: int=None) -> pd.Series:
        series = pd.Series(
            [1, 2, 3],
            index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
        )
        series.options = {'option': option}
        return series

    tsh.register_formula(
        engine,
        'test_options',
        '(* 3 (dummy #:option 42))',
        False
    )

    ts = tsh.get(engine, 'test_options')
    assert_df("""
2019-01-01    3
2019-01-02    6
2019-01-03    9
""", ts)
    assert ts.options == {'option': 42}

    FUNCS.pop('dummy')


def test_error(engine, tsh):
    with pytest.raises(SyntaxError):
        tsh.register_formula(
            engine,
            'test_error',
            '(clip (series "a")'
        )

    with pytest.raises(ValueError) as err:
        tsh.register_formula(
            engine,
            'test_error',
            '(priority (series "NOPE1") (series "NOPE2"))'
        )
    assert err.value.args[0] == (
        'Formula `test_error` refers to '
        'unknown series `NOPE1`, `NOPE2`'
    )


def test_history(engine, tsh):
    for day in (1, 2, 3):
        idate = utcdt(2019, 1, day)
        for name in 'ab':
            ts = pd.Series(
                [day] * 3,
                index=pd.date_range(dt(2018, 1, 1), periods=3, freq='D')
            )
            tsh.update(engine, ts, 'h' + name, 'Babar',
                       insertion_date=idate)

    tsh.register_formula(
        engine,
        'h-addition',
        '(add (series "ha") (series "hb"))'
    )

    h = tsh.history(engine, 'h-addition')
    assert_hist("""
insertion_date             value_date
2019-01-01 00:00:00+00:00  2018-01-01    2.0
                           2018-01-02    2.0
                           2018-01-03    2.0
2019-01-02 00:00:00+00:00  2018-01-01    4.0
                           2018-01-02    4.0
                           2018-01-03    4.0
2019-01-03 00:00:00+00:00  2018-01-01    6.0
                           2018-01-02    6.0
                           2018-01-03    6.0
""", h)

    dates = tsh.insertion_dates(engine, 'h-addition')
    assert dates == [
        pd.Timestamp('2019-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2019-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2019-01-03 00:00:00+0000', tz='UTC')
    ]

    h = tsh.history(
        engine, 'h-addition',
        from_insertion_date=utcdt(2019, 1, 2),
        to_insertion_date=utcdt(2019, 1, 2),
        from_value_date=dt(2018, 1, 2),
        to_value_date=dt(2018, 1, 2)
    )
    assert_hist("""
insertion_date             value_date
2019-01-02 00:00:00+00:00  2018-01-02    4.0
""", h)

    for day in (1, 2, 3):
        idate = utcdt(2019, 1, day)
        ts = pd.Series(
            [41 + day] * 3,
            index=pd.date_range(dt(2018, 1, 3), periods=3, freq='D')
        )
        tsh.update(engine, ts, 'hz', 'Babar',
                   insertion_date=idate)

    # let's add a priority
    tsh.register_formula(
        engine,
        'h-priority',
        '(priority (series "hz") (series "h-addition"))'
    )

    h = tsh.history(engine, 'h-priority')
    assert_hist("""
insertion_date             value_date
2019-01-01 00:00:00+00:00  2018-01-01     2.0
                           2018-01-02     2.0
                           2018-01-03    42.0
                           2018-01-04    42.0
                           2018-01-05    42.0
2019-01-02 00:00:00+00:00  2018-01-01     4.0
                           2018-01-02     4.0
                           2018-01-03    43.0
                           2018-01-04    43.0
                           2018-01-05    43.0
2019-01-03 00:00:00+00:00  2018-01-01     6.0
                           2018-01-02     6.0
                           2018-01-03    44.0
                           2018-01-04    44.0
                           2018-01-05    44.0
""", h)

    h = tsh.history(engine, 'h-priority', diffmode=True)
    assert_hist("""
insertion_date             value_date
2019-01-01 00:00:00+00:00  2018-01-01     2.0
                           2018-01-02     2.0
                           2018-01-03    42.0
                           2018-01-04    42.0
                           2018-01-05    42.0
2019-01-02 00:00:00+00:00  2018-01-01     4.0
                           2018-01-02     4.0
                           2018-01-03    43.0
                           2018-01-04    43.0
                           2018-01-05    43.0
2019-01-03 00:00:00+00:00  2018-01-01     6.0
                           2018-01-02     6.0
                           2018-01-03    44.0
                           2018-01-04    44.0
                           2018-01-05    44.0
""", h)


def test_history_with_spurious_keys(engine, tsh):
    """
    in certain case, the formula can produce histories
    with keys that index empty series
    """
    i0 = utcdt(2023, 3, 1)
    i1 = utcdt(2023, 3, 2)
    lb = dt(2023, 3, 1)
    ub = dt(2023, 3, 4)
    ts = pd.Series(range(4), index=pd.date_range(start=lb, end=ub, periods=4))

    # at date i0, only series-x is defined
    tsh.update(engine, ts, 'series-x', 'test', insertion_date=i0)

    tsh.update(engine, ts, 'series-y', 'test', insertion_date=i1)

    formula = """(add (series "series-x") (series "series-y"))"""
    tsh.register_formula(engine, 'simple-addition', formula)

    hist = tsh.history(engine, 'simple-addition')
    assert len(hist) == 2
    first_key = list(hist.keys())[0]

    # the first key index an empty series
    # it should be removed
    assert len(hist[first_key]) == 0


def test_history_bounds(engine, tsh):
    # two series, one with a gap

    #    ^  h0      h1
    #    |
    #  i2| xxx     xxx
    #  i1| xxx
    #  i0| xxx     xxx

    h0 = [
        [pd.Series([1], index=[dt(2021, 4, 1)]), utcdt(2020, 1, 1)],
        [pd.Series([2], index=[dt(2021, 4, 1)]), utcdt(2020, 1, 2)],
        [pd.Series([3], index=[dt(2021, 4, 1)]), utcdt(2020, 1, 3)],
    ]
    h1 = [
        [pd.Series([10], index=[dt(2021, 4, 1)]), utcdt(2020, 1, 1)],
        [pd.Series([30], index=[dt(2021, 4, 1)]), utcdt(2020, 1, 3)],
    ]

    for idx, h in enumerate([h0, h1]):
        for ts, idate in h:
            tsh.update(engine, ts, f'h{idx}', 'timemaster', insertion_date=idate)

    formula = '(add (series "h0") (series "h1"))'
    tsh.register_formula(engine, 'sum-h', formula)

    hist_all = tsh.history(engine, 'sum-h')

    hist_slice = tsh.history(engine, 'sum-h',
                             from_insertion_date= utcdt(2020, 1, 1, 12),
                             to_insertion_date = utcdt(2020, 1, 2, 12),
    )
    hist_top = tsh.history(engine, 'sum-h',
                           from_insertion_date=utcdt(2020, 1, 1, 12),
    )

    assert_hist("""
insertion_date             value_date
2020-01-01 00:00:00+00:00  2021-04-01    11.0
2020-01-02 00:00:00+00:00  2021-04-01    12.0
2020-01-03 00:00:00+00:00  2021-04-01    33.0
""", hist_all)

    # for references, h0 and h1, the components of the sum
    assert_hist("""
insertion_date             value_date
2020-01-01 00:00:00+00:00  2021-04-01    1.0
2020-01-02 00:00:00+00:00  2021-04-01    2.0
2020-01-03 00:00:00+00:00  2021-04-01    3.0
""", tsh.history(engine, 'h0'))

    assert_hist("""
insertion_date             value_date
2020-01-01 00:00:00+00:00  2021-04-01    10.0
2020-01-03 00:00:00+00:00  2021-04-01    30.0
""", tsh.history(engine, 'h1'))

    # as expected
    assert_hist("""
insertion_date             value_date
2020-01-02 00:00:00+00:00  2021-04-01    12.0
""", hist_slice)

    # one line is missing
    assert_hist("""
insertion_date             value_date
2020-01-02 00:00:00+00:00  2021-04-01    12.0
2020-01-03 00:00:00+00:00  2021-04-01    33.0
""", hist_top)


def test_history_diffmode(engine, tsh):
    for i in range(1, 4):
        ts = pd.Series([i], index=[utcdt(2020, 1, i)])
        tsh.update(engine, ts, 'hdiff', 'Babar',
                   insertion_date=utcdt(2020, 1, i))

    tsh.register_formula(
        engine,
        'f-hdiff',
        '(series "hdiff")'
    )
    h = tsh.history(engine, 'f-hdiff', diffmode=True)
    assert_hist("""
insertion_date             value_date               
2020-01-01 00:00:00+00:00  2020-01-01 00:00:00+00:00    1.0
2020-01-02 00:00:00+00:00  2020-01-02 00:00:00+00:00    2.0
2020-01-03 00:00:00+00:00  2020-01-03 00:00:00+00:00    3.0
""", h)


def test_staircase(engine, tsh):
    tsh.register_formula(
        engine,
        's-addition',
        '(add (series "sa") (series "sb"))',
        False
    )

    for day in (1, 2, 3, 4, 5):
        idate = utcdt(2018, 1, day)
        for name in 'ab':
            ts = pd.Series(
                [day / 2.] * 5,
                index=pd.date_range(dt(2018, 1, day), periods=5, freq='D')
            )
            tsh.update(engine, ts, 's' + name, 'Babar',
                       insertion_date=idate)

    ts = tsh.staircase(engine, 's-addition', delta=pd.Timedelta(hours=12))
    assert_df("""
2018-01-02    1.0
2018-01-03    2.0
2018-01-04    3.0
2018-01-05    4.0
2018-01-06    5.0
2018-01-07    5.0
2018-01-08    5.0
2018-01-09    5.0
""", ts)

    # this is not allowed in the staircase fast-path
    # hence we will take the slow path
    @func('identity')
    def identity(series: pd.Series) -> pd.Series:
        return series

    tsh.register_formula(
        engine,
        'slow-down',
        '(identity (series "sa"))',
        False
    )

    tsh.register_formula(
        engine,
        's-addition-not-fast',
        '(add (series "slow-down") (series "sb"))',
        False
    )
    ts = tsh.staircase(
        engine,
        's-addition-not-fast',
        delta=pd.Timedelta(hours=12)
    )
    assert_df("""
2018-01-02    1.0
2018-01-03    2.0
2018-01-04    3.0
2018-01-05    4.0
2018-01-06    5.0
2018-01-07    5.0
2018-01-08    5.0
2018-01-09    5.0
""", ts)

    # cleanup
    FUNCS.pop('identity')


def test_new_func(engine, tsh):

    @func('identity')
    def identity(series: pd.Series) -> pd.Series:
        return series

    tsh.register_formula(
        engine,
        'identity',
        '(identity (series "id-a"))',
        False
    )

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, ts, 'id-a', 'Babar')

    ts = tsh.get(engine, 'identity')
    assert_df("""
2019-01-01    1.0
2019-01-02    2.0
2019-01-03    3.0
""", ts)

    # cleanup
    FUNCS.pop('identity')


def test_ifunc(engine, tsh):

    @func('shifter', auto=True)
    def shifter(__interpreter__,
               __from_value_date__,
               __to_value_date__,
               __revision_date__,
                name: str,
                days: int=0) -> pd.Series:
        args = __interpreter__.getargs.copy()
        fromdate = args.get('from_value_date')
        todate = args.get('to_value_date')
        if fromdate:
            args['from_value_date'] = fromdate + timedelta(days=days)
        if todate:
            args['to_value_date'] = todate + timedelta(days=days)

        return __interpreter__.get(name, args)

    @metadata('shifter')
    def shifter_metadata(cn, tsh, stree):
        return {
            stree[1]: tsh.internal_metadata(cn, stree[1])
        }

    @finder('shifter')
    def shifter_finder(cn, tsh, stree):
        return {
            stree[1]: stree
        }

    ts = pd.Series(
        [1, 2, 3, 4, 5],
        index=pd.date_range(dt(2019, 1, 1), periods=5, freq='D')
    )
    tsh.update(
        engine, ts, 'shiftme', 'Babar',
        insertion_date=utcdt(2019, 1, 1)
    )

    tsh.register_formula(
        engine,
        'shifting',
        '(+ 0 (shifter "shiftme" #:days -1))'
    )

    ts = tsh.get(engine, 'shifting')
    assert_df("""
2019-01-01    1.0
2019-01-02    2.0
2019-01-03    3.0
2019-01-04    4.0
2019-01-05    5.0
""", ts)

    ts = tsh.get(
        engine, 'shifting',
        from_value_date=dt(2019, 1, 3),
        to_value_date=dt(2019, 1, 4)
    )
    assert_df("""
2019-01-02    2.0
2019-01-03    3.0
""", ts)

    # now, history

    ts = pd.Series(
        [1, 2, 3, 4, 5],
        index=pd.date_range(dt(2019, 1, 2), periods=5, freq='D')
    )
    tsh.update(
        engine, ts, 'shiftme', 'Babar',
        insertion_date=utcdt(2019, 1, 2)

    )
    hist = tsh.history(
        engine, 'shifting'
    )
    assert_hist("""
insertion_date             value_date
2019-01-01 00:00:00+00:00  2019-01-01    1.0
                           2019-01-02    2.0
                           2019-01-03    3.0
                           2019-01-04    4.0
                           2019-01-05    5.0
2019-01-02 00:00:00+00:00  2019-01-01    1.0
                           2019-01-02    1.0
                           2019-01-03    2.0
                           2019-01-04    3.0
                           2019-01-05    4.0
                           2019-01-06    5.0
""", hist)

    hist = tsh.history(
        engine, 'shifting',
        from_value_date=dt(2019, 1, 3),
        to_value_date=dt(2019, 1, 4)
    )
    assert_hist("""
insertion_date             value_date
2019-01-01 00:00:00+00:00  2019-01-03    3.0
                           2019-01-04    4.0
2019-01-02 00:00:00+00:00  2019-01-03    2.0
                           2019-01-04    3.0
""", hist)

    # cleanup
    FUNCS.pop('shifter')


def test_newop_expansion(engine, tsh):
    @func('combine')
    def combine(__interpreter__, name1: str, name2: str) -> pd.Series:
        args = __interpreter__.getargs.copy()
        return (
            __interpreter__.get(name1, args) +
            __interpreter__.get(name2, args)
        )

    @metadata('combine')
    def combine_metadata(cn, tsh, stree):
        return {
            stree[1]: tsh.metadata(cn, stree[1])
        }

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, ts, 'base-comb', 'Babar')

    tsh.register_formula(
        engine,
        'comb-a',
        '(add (series "base-comb") (series "base-comb"))'
    )
    tsh.register_formula(
        engine,
        'comb-b',
        '(priority (series "base-comb") (series "base-comb"))'
    )

    tsh.register_formula(
        engine,
        'combinator',
        '(combine "comb-a" "comb-b")',
        False
    )

    exp = tsh.expanded_formula(engine, 'combinator')
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (combine "comb-a" "comb-b")'
        ')'
    )

    exp = tsh.expanded_formula(
        engine,
        'combinator',
        revision_date=pd.Timestamp('2022-1-1'),
        to_value_date=pd.Timestamp('2030-1-1', tz='UTC')
    )
    assert lisp.parse(exp) == [
        'let',
        'revision_date', ['date', '2022-01-01T00:00:00', None],
        'from_value_date', None,
        'to_value_date', ['date', '2030-01-01T00:00:00+00:00', 'UTC'],
        ['combine', 'comb-a', 'comb-b']
    ]


def test_formula_refers_to_nothing(engine, tsh):
    tsh.register_formula(
        engine,
        'i-cant-work',
        '(+ 1 (series "lol"))',
        False
    )

    with pytest.raises(ValueError) as err:
        tsh.get(engine, 'i-cant-work')
    assert err.value.args[0] == 'No such series `lol`'


def test_rename(engine, tsh):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, ts, 'rename-a', 'Babar')

    tsh.register_formula(
        engine,
        'survive-renaming',
        '(+ 1 (series "rename-a" #:fill 0))'
    )
    tsh.register_formula(
        engine,
        'survive-renaming-2',
        '(add (series "survive-renaming") (series "rename-a" #:fill 0))'
    )

    ts = tsh.get(engine, 'survive-renaming')
    assert_df("""
2019-01-01    2.0
2019-01-02    3.0
2019-01-03    4.0
""", ts)

    ts = tsh.get(engine, 'survive-renaming-2')
    assert_df("""
2019-01-01    3.0
2019-01-02    5.0
2019-01-03    7.0
""", ts)

    with pytest.raises(AssertionError):
        with engine.begin() as cn:
            tsh.rename(cn, 'rename-a', ' ')

    with engine.begin() as cn:
        tsh.rename(cn, 'rename-a', 'a-renamed')

    ts = tsh.get(engine, 'survive-renaming')
    assert_df("""
2019-01-01    2.0
2019-01-02    3.0
2019-01-03    4.0
""", ts)

    ts = tsh.get(engine, 'survive-renaming-2')
    assert_df("""
2019-01-01    3.0
2019-01-02    5.0
2019-01-03    7.0
""", ts)

    with engine.begin() as cn:
        with pytest.raises(ValueError) as err:
            tsh.rename(cn, 'a-renamed', 'survive-renaming')

    assert err.value.args[0] == 'new name is already referenced by `survive-renaming-2`'

    # rename a formula !
    with engine.begin() as cn:
        tsh.rename(cn, 'survive-renaming', 'survived')
    assert tsh.formula(
        engine, 'survive-renaming-2'
    ) == '(add (series "survived") (series "a-renamed" #:fill 0))'


def test_unknown_operator(engine, tsh):
    with pytest.raises(ValueError) as err:
        tsh.register_formula(
            engine,
            'nope',
            '(bogus-1 (bogus-2))',
            False
        )

    assert err.value.args[0] == (
        'Formula `nope` refers to unknown operators `bogus-1`, `bogus-2`'
    )


def test_custom_metadata(engine, tsh):
    @func('customseries')
    def customseries() -> pd.Series:
        return pd.Series(
            [1.0, 2.0, 3.0],
            index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
        )

    @metadata('customseries')
    def customseries_metadata(_cn, _tsh, tree):
        return {
            tree[0]: {
                'index_type': 'datetime64[ns]',
                'index_dtype': '|M8[ns]',
                'tzaware': False,
                'value_type': 'float64',
                'value_dtype': '<f8'
            }
        }

    tsh.register_formula(
        engine,
        'custom',
        '(+ 3 (customseries))',
        False
    )

    meta = tsh.internal_metadata(engine, 'custom')
    assert meta == {
        'contenthash': '5c0ec5006648ef40a7358294233db59bebbacda6',
        'formula': '(+ 3 (customseries))',
        'index_type': 'datetime64[ns]',
        'index_dtype': '<M8[ns]',
        'tzaware': False,
        'value_type': 'float64',
        'value_dtype': '<f8'
    }

    # cleanup
    FUNCS.pop('customseries')


def test_custom_history(engine, tsh):
    @func('made-up-series', auto=True)
    def madeup(__interpreter__,
               __from_value_date__,
               __to_value_date__,
               __revision_date__,
               base: int,
               coeff: float=1.) -> pd.Series:
        return pd.Series(
            np.array([base, base + 1, base + 2]) * coeff,
            index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
        )

    @metadata('made-up-series')
    def madeup_metadata(_cn, _tsh, tree):
        return {
            tree[0]: {
                'index_type': 'datetime64[ns]',
                'index_dtype': '|M8[ns]',
                'tzaware': False,
                'value_type': 'float64',
                'value_dtype': '<f8'
            }
        }

    @history('made-up-series')
    def madeup_history(__interpreter__, base, coeff=1.):
        hist = {}
        for i in (1, 2, 3):
            hist[pd.Timestamp(f'2020-1-{i}', tz='utc')] = pd.Series(
                np.array([base + i, base + i + 1, base + i + 2]) * coeff,
                index=pd.date_range(dt(2019, 1, i), periods=3, freq='D')
            )
        return hist

    tsh.register_formula(
        engine,
        'made-up-history',
        '(made-up-series 0)'
    )

    assert_df("""
2019-01-01    0.0
2019-01-02    1.0
2019-01-03    2.0
""", tsh.get(engine, 'made-up-history'))

    assert_hist("""
insertion_date             value_date
2020-01-01 00:00:00+00:00  2019-01-01    1.0
                           2019-01-02    2.0
                           2019-01-03    3.0
2020-01-02 00:00:00+00:00  2019-01-02    2.0
                           2019-01-03    3.0
                           2019-01-04    4.0
2020-01-03 00:00:00+00:00  2019-01-03    3.0
                           2019-01-04    4.0
                           2019-01-05    5.0
""", tsh.history(engine, 'made-up-history'))

    idates = tsh.insertion_dates(engine, 'made-up-history')
    assert idates == [
        pd.Timestamp('2020-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2020-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2020-01-03 00:00:00+0000', tz='UTC')
    ]

    tsh.register_formula(
        engine,
        'made-up-composite',
        '(+ 3 (add (made-up-series 1 #:coeff 2.) (made-up-series 2 #:coeff .5)))',
        False
    )
    assert_df("""
2019-01-01     6.0
2019-01-02     8.5
2019-01-03    11.0
""", tsh.get(engine, 'made-up-composite'))

    hist = tsh.history(engine, 'made-up-composite')
    assert_hist("""
insertion_date             value_date
2020-01-01 00:00:00+00:00  2019-01-01     8.5
                           2019-01-02    11.0
                           2019-01-03    13.5
2020-01-02 00:00:00+00:00  2019-01-02    11.0
                           2019-01-03    13.5
                           2019-01-04    16.0
2020-01-03 00:00:00+00:00  2019-01-03    13.5
                           2019-01-04    16.0
                           2019-01-05    18.5
""", hist)


def test_autotrophic_operators_history(engine, tsh):
    with pytest.raises(AssertionError) as err:
        @func('test-path', auto=True)
        def test_path(__interpreter__,
                      __from_value_date__,
                      __revision_date__,
                      base: int,
                      coeff: float=1.) -> pd.Series:

            # We don't get there
            assert False

    assert err.value.args[0] == (
        '`test-path` is an autotrophic operator. '
        'It should have a `__to_value_date__` positional argument.'
    )


def test_expanded(engine, tsh):
    @func('customseries')
    def customseries() -> pd.Series:
        return pd.Series(
            [1.0, 2.0, 3.0],
            index=pd.date_range(dt(2019, 1, 1), periods=3, freq='D')
        )

    @metadata('customseries')
    def customseries_metadata(_cn, _tsh, tree):
        return {
            tree[0]: {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

    base = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2019, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, base, 'exp-a', 'Babar')
    tsh.update(engine, base, 'exp-b', 'Celeste')

    tsh.register_formula(
        engine,
        'expandmebase1',
        '(+ 3 (priority (series "exp-a") (customseries)))',
        False
    )
    tsh.register_formula(
        engine,
        'expandmebase2',
        '(priority (series "exp-a") (series "exp-b"))',
        False
    )
    tsh.register_formula(
        engine,
        'expandme',
        '(add (series "expandmebase1") (series "exp-b") (series "expandmebase2"))',
        False
    )

    exp = tsh.expanded_formula(engine, 'expandme')
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (add'
        ' (+ 3 (priority (series "exp-a") (customseries)))'
        ' (series "exp-b")'
        ' (priority (series "exp-a") (series "exp-b")))'
        ')'
    )


def test_slice_naive(engine, tsh):
    ts_hourly = pd.Series(
        [1.0] * 24 * 3,
        index=pd.date_range(utcdt(2022, 4, 1), periods=24 * 3, freq='H')
    )
    tsh.update(engine, ts_hourly, 'ts.hourly', 'test')

    formula = (
        '(slice '
        ' (naive (series "ts.hourly") "CET")'
        ' #:fromdate (date "2022-01-01")'
        ')'
    )
    tsh.register_formula(engine, 'slice.naive', formula)

    ts = tsh.get(
        engine,
        'slice.naive',
        from_value_date=dt(2022, 4, 2),
        to_value_date=dt(2022, 4, 3),
    )
    assert ts.index.min() == pd.Timestamp('2022-04-02 02:00:00')
    assert ts.index.max() == pd.Timestamp('2022-04-03 00:00:00')


def test_history_nr(engine, tsh):
    ts1 = pd.Series(
        [1.0] * 24,
        index=pd.date_range(utcdt(2020, 1, 1), periods=24, freq='H')
    )

    tsh.update(
        engine,
        ts1,
        'hist-nr-1',
        'Babar',
        insertion_date=utcdt(2020, 1, 1)
    )
    tsh.update(
        engine,
        ts1 + 1,
        'hist-nr-2',
        'Babar',
        insertion_date=utcdt(2020, 1, 1, 1)
    )

    ts2 = pd.Series(
        [10] * 24,
        index=pd.date_range(utcdt(2020, 1, 1), periods=24, freq='H')
    )

    tsh.update(
        engine,
        ts2,
        'hist-nr-1',
        'Babar',
        insertion_date=utcdt(2020, 1, 2)
    )
    tsh.update(
        engine,
        ts2 + 1,
        'hist-nr-2',
        'Babar',
        insertion_date=utcdt(2020, 1, 2, 1)
    )

    tsh.register_formula(
        engine,
        'hist-nr-form',
        '(row-mean (resample (naive (series "hist-nr-1") "CET") "D") '
        '          (resample (naive (series "hist-nr-2") "CET") "D")) '
    )

    top = tsh.get(engine, 'hist-nr-form')
    assert_df("""
2020-01-01    10.5
2020-01-02    10.5
""", top)


    hist = tsh.history(engine, 'hist-nr-form')
    assert_hist("""
insertion_date             value_date
2020-01-01 00:00:00+00:00  2020-01-01     1.0
                           2020-01-02     1.0
2020-01-01 01:00:00+00:00  2020-01-01     1.5
                           2020-01-02     1.5
2020-01-02 00:00:00+00:00  2020-01-01     6.0
                           2020-01-02     6.0
2020-01-02 01:00:00+00:00  2020-01-01    10.5
                           2020-01-02    10.5
""", hist)


def test_slice_tzaware(engine, tsh):
    tomorwow = dt.now().date() + timedelta(days=1)
    tomorwow_tz = pd.Timestamp(tomorwow, tz='UTC')
    begin_tz = tomorwow_tz - timedelta(days=2)

    index = pd.date_range(
        start=begin_tz,
        end=tomorwow_tz,
        freq='H'
    )
    ts = pd.Series(
        [1.0] * len(index),
        index=index,
    )
    tsh.update(engine, ts, 'whocares', 'test')

    tsh.register_formula(
        engine,
        'slice_tz',
        '(naive (slice (series "whocares")  #:todate (today)) "CET")'
    )

    intermediary_utc = begin_tz + timedelta(days=1)

    # without bounds => no error
    tsh.get(engine, 'naive_slice')

    # with naive bound => no error
    tsh.get(engine, 'naive_slice', from_value_date=dt.now().date())
    # with utc bound => no error
    tsh.get(engine, 'slice_tz', from_value_date=intermediary_utc)

    intermediary_cet = intermediary_utc.tz_convert('CET')
    # with CET bounds => no error
    tsh.get(engine, 'slice_tz', from_value_date=intermediary_cet)


def test_history_autotrophic_nr(engine, tsh):
    # reset this to be sure it contains our _very late_ new operators definitions
    OperatorHistory.FUNCS = None

    ts1 = pd.Series(
        [1.0] * 24,
        index=pd.date_range(utcdt(2020, 1, 1), periods=24, freq='H')
    )
    ts2 = pd.Series(
        [10] * 24,
        index=pd.date_range(utcdt(2020, 1, 1), periods=24, freq='H')
    )

    @func('hist-nr2-1', auto=True)
    def histnr21(__interpreter__,
                 __from_value_date__,
                 __to_value_date__,
                 __revision_date__) -> pd.Series:
        return ts2

    @metadata('hist-nr2-1')
    def histnr21_metadata(_cn, _tsh, tree):
        return {
            tree[0]: {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

    @history('hist-nr2-1')
    def histnr21history(__interpreter__):
        return {
            utcdt(2020, 1, 1): ts1,
            utcdt(2020, 1, 2): ts2
        }

    @func('hist-nr2-2', auto=True)
    def histnr22(__interpreter__,
                 __from_value_date__,
                 __to_value_date__,
                 __revision_date__) -> pd.Series:
        return ts2 + 1

    @metadata('hist-nr2-2')
    def histnr22_metadata(_cn, _tsh, tree):
        return {
            tree[0]: {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

    @history('hist-nr2-2')
    def histnr22history(__interpreter__):
        return {
            utcdt(2020, 1, 1, 1): ts1 + 1,
            utcdt(2020, 1, 2, 1): ts2 + 1
        }

    tsh.register_formula(
        engine,
        'hist-nr-form2',
        '(row-mean (resample (naive (hist-nr2-1) "CET") "D") '
        '          (resample (naive (hist-nr2-2) "CET") "D"))'
    )

    top = tsh.get(engine, 'hist-nr-form2')
    assert_df("""
2020-01-01    10.5
2020-01-02    10.5
""", top)

    hist = tsh.history(engine, 'hist-nr-form2')
    assert_hist("""
insertion_date             value_date
2020-01-01 00:00:00+00:00  2020-01-01     1.0
                           2020-01-02     1.0
2020-01-01 01:00:00+00:00  2020-01-01     1.5
                           2020-01-02     1.5
2020-01-02 00:00:00+00:00  2020-01-01     6.0
                           2020-01-02     6.0
2020-01-02 01:00:00+00:00  2020-01-01    10.5
                           2020-01-02    10.5
""", hist)


def test_history_auto_nr(engine, tsh):
    OperatorHistory.FUNCS = None

    ts1 = pd.Series(
        [1.0] * 24,
        index=pd.date_range(utcdt(2020, 1, 1), periods=24, freq='H')
    )
    ts2 = pd.Series(
        [10] * 24,
        index=pd.date_range(utcdt(2020, 1, 1), periods=24, freq='H')
    )

    @func('auto-operator', auto=True)
    def auto_operator(__interpreter__,
                      __from_value_date__,
                      __to_value_date__,
                      __revision_date__) -> pd.Series:
        return ts2

    @metadata('auto-operator')
    def auto_operator_metadata(_cn, _tsh, tree):
        return {
            tree[0]: {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

    @finder('auto-operator')
    def auto_operator_finder(cn, tsh, tree):
        return {'auto-operator': tree}

    @history('auto-operator')
    def auto_operator_history(__interpreter__):
        return {
            utcdt(2020, 1, 1): ts1,
            utcdt(2020, 1, 2): ts2
        }

    tsh.register_formula(engine, 'auto_series', '(auto-operator)')
    tsh.get(engine, 'auto_series')
    hist = tsh.history(
        engine,
        'auto_series',
        from_insertion_date=utcdt(2020, 1, 1, 12)
    )
    assert len(hist) == 2


def test_forged_names():

    def func(name):
        def decorator(func):
            def wrapper(func, *a, **kw):
                fname = _name_from_signature_and_args(name, func, a, kw)
                return fname
            dec = decorate(func, wrapper)
            return dec
        return decorator

    @func('foo')
    def foo(a, b=42):
        return a + b

    assert foo(1) == 'foo-a=1-b=42'
    assert foo(1, 43) == 'foo-a=1-b=43'
    assert foo(a=1) == 'foo-a=1-b=42'
    assert foo(b=43, a=1) == 'foo-a=1-b=43'


def test_extract_from_expr(engine, tsh):

    @func('extractme')
    def extractme(
            __a, __b, __c, __d,
            a: str, b: int, date: pd.Timestamp, bar: str) -> type(None):
        pass

    tree = lisp.parse('(extractme "a" 42 #:bar "bar" #:date (date "2021-1-1"))')
    fname, f, args, kw = _extract_from_expr(tree)
    assert fname == 'extractme'
    assert f == extractme
    assert isinstance(args[0], NullIntepreter)
    assert args[4:] == ['a', 42]
    assert kw == {'bar': 'bar', 'date': '(date "2021-1-1")'}

    it = Interpreter(engine, tsh, {})
    tree = lisp.parse('(extractme "a" 42 #:bar "bar" #:date (date "2021-1-1"))')
    fname, f, args, kw = _extract_from_expr(tree)
    assert fname == 'extractme'
    assert f == extractme
    assert isinstance(args[0], NullIntepreter)
    assert args[4:] == ['a', 42]
    assert kw == {
        'bar': 'bar',
        'date': '(date "2021-1-1")'
    }

    assert name_of_expr(tree) == 'extractme-a=a-b=42-date=(date "2021-1-1")-bar=bar'


def test_history_auto_name_issue(engine, tsh):
    # reset this to be sure it contains our _very late_ new operators definitions
    OperatorHistory.FUNCS = None
    ts = pd.Series(
        [1.0, 2.0, 3.0],
        index=pd.date_range(utcdt(2020, 1, 1), periods=3, freq='D')
    )

    @func('hist-auto-name', auto=True)
    def histautoname(__interpreter__,
                     __from_value_date__,
                     __to_value_date__,
                     __revision_date__,
                     a:int,
                     b:int=0) -> pd.Series:
        return (ts + a + b) * 2

    fname, funcobj, args, kwargs = _extract_from_expr(lisp.parse('(hist-auto-name 0 #:b 1)'))
    assert fname == 'hist-auto-name'
    assert args[1:] == [ # the NullIntepreter is not even comparable to itself, let's skip it
        None,
        None,
        None,
        0
    ]
    assert kwargs == {'b': 1}

    forgedname = _name_from_signature_and_args(fname, funcobj, args, kwargs)
    assert forgedname == 'hist-auto-name-a=0-b=1'

    @metadata('hist-auto-name')
    def histautoname_metadata(_cn, _tsh, tree):
        return {
            tree[0]: {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

    @history('hist-auto-name')
    def histautoname_history(__interpreter__, a:int=0):
        return {
            utcdt(2020, 1, 1): ts + a,
            utcdt(2020, 1, 2): (ts + a) * 2
        }

    tsh.register_formula(
        engine,
        'good-auto-history',
        '(add (hist-auto-name 0) '
        '     (hist-auto-name 1))'
    )

    top = tsh.get(engine, 'good-auto-history')
    assert_df("""
2020-01-01 00:00:00+00:00     6.0
2020-01-02 00:00:00+00:00    10.0
2020-01-03 00:00:00+00:00    14.0
""", top)

    hist = tsh.history(engine, 'good-auto-history')
    assert_hist("""
insertion_date             value_date               
2020-01-01 00:00:00+00:00  2020-01-01 00:00:00+00:00     3.0
                           2020-01-02 00:00:00+00:00     5.0
                           2020-01-03 00:00:00+00:00     7.0
2020-01-02 00:00:00+00:00  2020-01-01 00:00:00+00:00     6.0
                           2020-01-02 00:00:00+00:00    10.0
                           2020-01-03 00:00:00+00:00    14.0
""", hist)


def test_history_auto_name_subexpr(engine, tsh):
    # reset this to be sure it contains our _very late_ new operators definitions
    OperatorHistory.FUNCS = None
    ts = pd.Series(
        [1.0, 2.0, 3.0],
        index=pd.date_range(utcdt(2020, 1, 1), periods=3, freq='D')
    )

    @func('hist-auto-subexpr', auto=True)
    def histautoname(__interpreter__,
                     __from_value_date__,
                     __to_value_date__,
                     __revision_date__,
                     date: pd.Timestamp) -> pd.Series:
        return ts

    @metadata('hist-auto-subexpr')
    def histautoname_metadata(_cn, _tsh, tree):
        return {
            tree[0]: {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        }

    @history('hist-auto-subexpr')
    def histautoname_history(__interpreter__, date: pd.Timestamp):
        return {
            utcdt(2020, 1, 1): ts,
            utcdt(2020, 1, 2): ts + 1
        }

    tsh.register_formula(
        engine,
        'auto-history-sub',
        '(add (hist-auto-subexpr (today)) '
        '     (hist-auto-subexpr #:date (date "2020-1-2")))'
    )

    top = tsh.get(engine, 'auto-history-sub')
    assert_df("""
2020-01-01 00:00:00+00:00    2.0
2020-01-02 00:00:00+00:00    4.0
2020-01-03 00:00:00+00:00    6.0
""", top)

    hist = tsh.history(engine, 'auto-history-sub')
    assert_hist("""
insertion_date             value_date               
2020-01-01 00:00:00+00:00  2020-01-01 00:00:00+00:00    2.0
                           2020-01-02 00:00:00+00:00    4.0
                           2020-01-03 00:00:00+00:00    6.0
2020-01-02 00:00:00+00:00  2020-01-01 00:00:00+00:00    4.0
                           2020-01-02 00:00:00+00:00    6.0
                           2020-01-03 00:00:00+00:00    8.0
""", hist)


def test_expand_vs_fill(engine, tsh):
    ts = pd.Series(
        [1.0, 2.0, 3.0],
        index=pd.date_range(utcdt(2021, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        ts,
        'base-expand-me',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'bottom-expandme',
        '(series "base-expand-me")'
    )

    tsh.register_formula(
        engine,
        'top-expandme',
        '(row-mean (series "bottom-expandme" #:fill 0 #:weight 1.5) '
        '          (series "bottom-expandme" #:fill 1))'
    )

    e = expanded(
        tsh,
        engine,
        lisp.parse(tsh.formula(engine, 'top-expandme'))
    )

    assert lisp.serialize(e) == (
        '(row-mean'
        ' (options (series "base-expand-me") #:fill 0 #:weight 1.5)'
        ' (options (series "base-expand-me") #:fill 1))'
    )

    ts = tsh.get(
        engine,
        'top-expandme',
        to_value_date=utcdt(2021, 1, 5)
    )

    # careful reading that: row-mean does not use #:fill
    # and handles itself well the missing value situation
    # the #:weight is also a bit useless if only for the
    # above test
    assert_df("""
2021-01-01 00:00:00+00:00    1.0
2021-01-02 00:00:00+00:00    2.0
2021-01-03 00:00:00+00:00    3.0
""", ts)


def test_expanded_stopnames(engine, tsh):
    ts = pd.Series(
        [1.0, 2.0, 3.0],
        index=pd.date_range(utcdt(2021, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        ts,
        'base-expand-me2',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'bottom-expandme2',
        '(series "base-expand-me2")'
    )

    tsh.register_formula(
        engine,
        'bottom-expandme3',
        '(series "base-expand-me2")'
    )

    tsh.register_formula(
        engine,
        'top-expandme2',
        '(row-mean (series "bottom-expandme2" #:fill 0 #:weight 1.5) '
        '          (series "bottom-expandme3" #:fill 1))'
    )

    e = expanded(
        tsh,
        engine,
        lisp.parse(tsh.formula(engine, 'top-expandme2')),
        stopnames=('bottom-expandme2',)
    )

    assert lisp.serialize(e) == (
        '(row-mean'
        ' (series "bottom-expandme2" #:fill 0 #:weight 1.5)'
        ' (options (series "base-expand-me2") #:fill 1))'
    )


def test_expanded_shownames(engine, tsh):
    """
    This test presents the shownames option of expanded_formula
    Shownames is an iterable of series names
    Principle: starting from the root, the (named) nodes are iteratively
    expanded, if and only if, one of the shownames belong on their descendants.

    Our tree formula will be:
    A-+-B--C
      +-D--E--F--G
    The shownames are ['E']
    So the result should look like:
    A-+-B
      +-D--E
    """
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2022, 1, 1), periods=3, freq='D')
    )

    # primaries:
    tsh.update(engine, ts, 'show-c', 'test')
    tsh.update(engine, ts, 'show-g', 'test')

    # formulas:
    tsh.register_formula(engine, 'show-b', '(* 2 (series "show-c"))')
    tsh.register_formula(engine, 'show-f', '(* 1 (series "show-g"))')
    tsh.register_formula(engine, 'show-e', '(* 3 (series "show-f"))')
    tsh.register_formula(engine, 'show-d', '(* 4 (series "show-e"))')
    tsh.register_formula(
        engine,
        'show-a',
        '(add (series "show-b") (series "show-d"))'
    )

    assert has_names(
        tsh,
        engine,
        lisp.parse('(series "show-d")'),
        ('show-f',),
        ()
    )
    assert not has_names(
        tsh,
        engine,
        lisp.parse(tsh.formula(engine, 'show-d')),
        ('nawak',),
        ()
    )
    formula_semi_expanded = expanded(
        tsh,
        engine,
        lisp.parse(tsh.formula(engine, 'show-a')),
        shownames=('show-e',)
    )

    assert lisp.serialize(formula_semi_expanded) == (
        '(add'
        ' (series "show-b")'
        ' (* 4 (series "show-e")))'
    )

    """
    New case:
    Same tree
    A-+-B--C
      +-D--E--F--G
    The shownames are now ['D']
    So the result should look like:
    A-+-B
      +-D
    """
    formula_semi_expanded = expanded(
        tsh,
        engine,
        lisp.parse(tsh.formula(engine, 'show-a')),
        shownames=('show-d',)
    )

    assert lisp.serialize(formula_semi_expanded) == (
        '(add'
        ' (series "show-b")'
        ' (series "show-d"))'
    )


def test_autolike_operator_history_nr(engine, tsh):
    """ In which we show that an history call of an operator playing with
    interpreter args will NOT crash with a lack of an internal
    revision_date
    """

    @func('weird-operator')
    def weirdo(__interpreter__, name: str) -> pd.Series:
        assert 'revision_date' in __interpreter__.getargs
        return __interpreter__.get(
            name,
            __interpreter__.getargs
        )

    @metadata('weird-operator')
    def weirdo(cn, tsh, tree):
        return {
            tree[1]: tsh.internal_metadata(cn, tree[1])
        }

    @finder('weird-operator')
    def weirdo(cn, tsh, tree):
        return {
            tree[1]: tree
        }

    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(
            pd.Timestamp('2022-1-1'), periods=3, freq='D'
        )
    )

    tsh.update(
        engine,
        ts,
        'today-base',
        'Babar',
        insertion_date=pd.Timestamp('2022-1-1', tz='utc')
    )

    tsh.register_formula(
        engine,
        'weird-operator',
        '(weird-operator "today-base")'
    )

    hist = tsh.history(
        engine,
        'weird-operator'
    )
    assert_hist("""
insertion_date             value_date
2022-01-01 00:00:00+00:00  2022-01-01    1.0
                           2022-01-02    2.0
                           2022-01-03    3.0
""", hist)


def test_dependants(engine, tsh):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2022, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        ts,
        'dep-base',
        'Babar'
    )

    tsh.register_formula(
        engine,
        'dep-bottom',
        '(series "dep-base")'
    )
    tsh.register_formula(
        engine,
        'dep-middle-left',
        '(+ -1 (series "dep-bottom"))'
    )
    tsh.register_formula(
        engine,
        'dep-middle-right',
        '(+ 1 (series "dep-bottom"))'
    )
    tsh.register_formula(
        engine,
        'dep-top',
        '(add (series "dep-middle-left") (series "dep-middle-right"))'
    )
    assert tsh.dependents(engine, 'dep-top') == []
    assert tsh.dependents(engine, 'dep-middle-left') == [
        'dep-top',
    ]
    assert tsh.dependents(engine, 'dep-middle-right') == [
        'dep-top',
    ]
    assert tsh.dependents(engine, 'dep-bottom') == [
        'dep-middle-left',
        'dep-middle-right',
        'dep-top'
    ]

    assert tsh.dependents(engine, 'dep-bottom', direct=True) == [
        'dep-middle-left',
        'dep-middle-right'
    ]

    # update and see

    tsh.register_formula(
        engine,
        'dep-bottom-2',  # an alternative to bottom
        '(series "dep-base")'
    )

    tsh.register_formula(
        engine,
        'dep-middle-left',
        '(+ -1 (series "dep-bottom-2"))'
    )
    tsh.register_formula(
        engine,
        'dep-top',
        '(add'
        ' (series "dep-middle-right")'
        ' (series "dep-middle-right"))'
    )
    assert tsh.dependents(engine, 'dep-bottom-2') == [
        'dep-middle-left',
    ]
    assert tsh.dependents(engine, 'dep-bottom') == [
        'dep-middle-right',
        'dep-top'
    ]

    # delete things and see
    tsh.delete(engine, 'dep-top')

    assert tsh.dependents(engine, 'dep-bottom') == [
        'dep-middle-right'
    ]

    tsh.delete(engine, 'dep-middle-right')
    assert tsh.dependents(engine, 'dep-middle-left') == []
    assert tsh.dependents(engine, 'dep-middle-right') == []


def test_transitive_closure_dependents(engine, tsh):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(utcdt(2022, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        ts,
        'dep-base-2',
        'Babar'
    )
    tsh.register_formula(
        engine,
        'dep-f1',
        '(series "dep-base-2")'
    )
    tsh.register_formula(
        engine,
        'dep-f2',
        '(series "dep-f1")'
    )
    tsh.register_formula(
        engine,
        'dep-f3',
        '(series "dep-f2")'
    )

    deps = tsh.dependents(engine, 'dep-f1')
    assert deps == ['dep-f2', 'dep-f3']


def test_fill_and_clip(engine, tsh):
    # setup:
    # 2 series one of length 2, the other 4
    # 'add' operator, with fill=0 => we expect that the formula would have a len of 4

    ts = pd.Series(
        [1] * 2,
        index=pd.date_range(start=dt(2022, 1, 1),freq='D', periods=2)
    )
    tsh.update(engine, ts, 'ts-1', 'toto')

    ts = pd.Series(
        [2] * 4,
        index=pd.date_range(start=dt(2022, 1, 1), freq='D', periods=4)
    )
    tsh.update(engine, ts, 'ts-2', 'tata')

    formula = (
        '(add'
        ' (series "ts-1" #:fill 0)'
        ' (series "ts-2" #:fill 0))'
    )

    tsh.register_formula(engine, 'without-clip', formula)

    assert len(tsh.get(engine, 'without-clip')) == 4

    # so far, so good
    # now, we introduce a clip operator that should have no effect both on the values
    # and the length of the formula result

    formula = (
        '(add'
        ' (clip (series "ts-1" #:fill 0) #:max 3000000000)'
        ' (series "ts-2"#:fill 0))'
    )

    tsh.register_formula(engine, 'with-clip', formula)

    assert len(tsh.get(engine, 'with-clip')) == 4
    assert tsh.get(engine, 'without-clip').equals(tsh.get(engine, 'with-clip'))

    # everything is fine, now. The equilibrium is restored ^-^


def test_diagnose(engine, tsh):
    """
    let's build some complex tree

    A-+-B-+-C(2 series)
      |   +-D(3 autotrophs)
      |
      + E-+-F---G(1 series)
          +-H(1 autotroph)

    We want to produce a number of statistics:
    - Number of named nodes         (7)
    - Max depth (from named nodes)  (4)
    - Numbers of individual series  (6)
        - from operator Series      (3) (with list of names
        - from autotrophic operators(3) (with list of operator calls)
            - operator a            (2)
            - operator b            (1)
    """
    OperatorHistory.FUNCS = None
    # series
    ts = pd.Series(
        [2] * 4,
        index=pd.date_range(
            start=dt(2022, 1, 1),
            freq='D',
            periods=4
        )
    )
    tsh.update(engine, ts, 'prim-diag-1', 'test')
    tsh.update(engine, ts, 'prim-diag-2', 'test')

    # autotrophics
    OperatorHistory.FUNCS = None

    @func('diag-auto-1', auto=True)
    def diag1(
            __interpreter__,
            __from_value_date__,
            __to_value_date__,
            __revision_date__,
            ref: str,
    ) -> pd.Series:
        return ts

    @finder('diag-auto-1')
    def diag1(cn, tsh, tree):
        return {f'diag-1': tree}

    @func('diag-auto-2', auto=True)
    def diag2(
            __interpreter__,
            __from_value_date__,
            __to_value_date__,
            __revision_date__,
            ref: str,
    ) -> pd.Series:
        return ts

    @finder('diag-auto-2')
    def diag2(cn, tsh, tree):
        return {f'diag-2': tree}

    formula = '(priority (series "prim-diag-1") (series "prim-diag-2"))'
    tsh.register_formula(engine, 'diag-C', formula)

    formula = '(priority (diag-auto-1 "id-a") (diag-auto-2 "id-b") (diag-auto-1 "id-c"))'
    tsh.register_formula(engine, 'diag-D', formula)

    formula = '(add (series "diag-C") (series "diag-D"))'
    tsh.register_formula(engine, 'diag-B', formula)

    formula = '(resample (series "prim-diag-1") "D")'
    tsh.register_formula(engine, 'diag-G', formula)

    formula = '(* 3.14 (series "diag-G"))'
    tsh.register_formula(engine, 'diag-F', formula)

    formula = '(diag-auto-1 "id-a")'
    tsh.register_formula(engine, 'diag-H', formula)

    formula = '(add (series "diag-F") (series "diag-H"))'
    tsh.register_formula(engine, 'diag-E', formula)

    formula = '(add (series "diag-B") (series "diag-E"))'
    tsh.register_formula(engine, 'diag-A', formula)

    all_autos = find_autos(
        engine,
        tsh,
        'diag-A',
    )

    assert all_autos == {
        ('diag-auto-1', 3, 2): [
            ('(diag-auto-1 "id-a")', 2),
            ('(diag-auto-1 "id-c")', 1)
        ],
        ('diag-auto-2', 1, 1): [
            ('(diag-auto-2 "id-b")', 1)
        ]
    }

    nodes = scan_descendant_nodes(engine, tsh, 'diag-A')

    assert nodes == {
        'degree': 4,
        ('named-nodes', 7, 7): [
            ('diag-B', 1),
            ('diag-C', 1),
            ('diag-D', 1),
            ('diag-E', 1),
            ('diag-F', 1),
            ('diag-G', 1),
            ('diag-H', 1)
        ],
        ('primaries', 3, 2):  [
            ('prim-diag-1', 2),
            ('prim-diag-2', 1)
        ]
    }

    # Note: we define a new convention: i.e. the "degree" of a formula
    # a formula that reference primary series has a degree of 1
    # a formula that reference a formula of degree n has a degree of n+1
    # by extension, the autotroph are of degree 0

    stats = tsh.formula_stats(engine, 'diag-A')
    assert stats == {
        'autotrophics':
            {
                ('diag-auto-1', 3, 2): [
                    ('(diag-auto-1 "id-a")', 2),
                    ('(diag-auto-1 "id-c")', 1)
                ],
                ('diag-auto-2', 1, 1): [
                    ('(diag-auto-2 "id-b")', 1)
                ]
            },
        'degree': 4,
        ('named-nodes', 7, 7): [
            ('diag-B', 1),
            ('diag-C', 1),
            ('diag-D', 1),
            ('diag-E', 1),
            ('diag-F', 1),
            ('diag-G', 1),
            ('diag-H', 1)
        ],
        ('primaries', 3, 2): [
            ('prim-diag-1', 2),
            ('prim-diag-2', 1)
        ]
    }


def test_formula_patch(engine, tsh):
    ts = pd.Series(
        range(5),
        index=pd.date_range(
            start=dt(2023, 1, 10),
            freq='D',
            periods=5
        )
    )
    tsh.update(engine, ts, 'series-sup-0', 'test')
    tsh.update(engine, ts, 'series-sup-1', 'test')

    formula = '(add (series "series-sup-0") (series "series-sup-1"))'

    tsh.register_formula(engine, 'form-to-supervise', formula)
    assert_df("""
2023-01-10    0.0
2023-01-11    2.0
2023-01-12    4.0
2023-01-13    6.0
2023-01-14    8.0
""", tsh.get(engine, 'form-to-supervise'))

    # we want to patch the 2 last values and add an extra point
    ts = pd.Series(
        [-1] * 3,
        index=pd.date_range(
            start=dt(2023, 1, 13),
            freq='D',
            periods=3
        )
    )
    tsh.update(engine, ts, 'form-to-supervise', 'supervisor')

    assert_df("""
2023-01-10    0.0
2023-01-11    2.0
2023-01-12    4.0
2023-01-13   -1.0
2023-01-14   -1.0
2023-01-15   -1.0
""", tsh.get(engine, 'form-to-supervise'))

    # rename: the patch must stay
    tsh.rename(engine, 'form-to-supervise', 'new-name')
    assert_df("""
2023-01-10    0.0
2023-01-11    2.0
2023-01-12    4.0
2023-01-13   -1.0
2023-01-14   -1.0
2023-01-15   -1.0
""", tsh.get(engine, 'new-name'))

    # delete and recreate: the patch must disappear
    tsh.delete(engine, 'new-name')
    tsh.register_formula(engine, 'new-name', formula)

    assert_df("""
2023-01-10    0.0
2023-01-11    2.0
2023-01-12    4.0
2023-01-13    6.0
2023-01-14    8.0
""", tsh.get(engine, 'new-name'))

    # patch deletion points:
    # we add the correction then
    # we remove the correction on two points with N/A
    ts = pd.Series(
        [-1] * 3,
        index=pd.date_range(
            start=dt(2023, 1, 13),
            freq='D',
            periods=3
        )
    )
    tsh.update(engine, ts, 'new-name', 'supervisor')

    ts = pd.Series(
        np.nan * 2,
        index=pd.date_range(
            start=dt(2023, 1, 13),
            freq='D',
            periods=2
        )
    )
    tsh.update(engine, ts, 'new-name', 'supervisor')

    assert_df("""
2023-01-10    0.0
2023-01-11    2.0
2023-01-12    4.0
2023-01-13    6.0
2023-01-14    8.0
2023-01-15   -1.0
""", tsh.get(engine, 'new-name'))


# groups

def test_group_formula(engine, tsh):
    df = gengroup(
        n_scenarios=3,
        from_date=dt(2015, 1, 1),
        length=5,
        freq='D',
        seed=2
    )

    colnames = ['a', 'b', 'c']
    df.columns = colnames

    tsh.group_replace(engine, df, 'group1', 'test')

    # first group formula, referencing a group
    tsh.register_group_formula(
        engine,
        'group_formula',
        '(group "group1")'
    )
    df = tsh.group_get(engine, 'group_formula')

    assert_df("""
              a    b    c
2015-01-01  2.0  3.0  4.0
2015-01-02  3.0  4.0  5.0
2015-01-03  4.0  5.0  6.0
2015-01-04  5.0  6.0  7.0
2015-01-05  6.0  7.0  8.0
""", df)

    tsh.group_replace(engine, df, 'group2', 'test')

    tsh.register_group_formula(
        engine,
        'sumgroup',
        '(group-add (group "group1") (group "group2"))'
    )

    df = tsh.group_get(engine, 'sumgroup')

    assert_df("""
               a     b     c
2015-01-01   4.0   6.0   8.0
2015-01-02   6.0   8.0  10.0
2015-01-03   8.0  10.0  12.0
2015-01-04  10.0  12.0  14.0
2015-01-05  12.0  14.0  16.0
""", df)

    plain_ts = pd.Series(
        [1] * 7,
        index=pd.date_range(
            start=dt(2014, 12, 31),
            freq='D',
            periods=7,
        )
    )
    tsh.update(engine, plain_ts, 'plain_ts', 'Babar')

    # group-add is polymorphic
    tsh.register_group_formula(
        engine,
        'mixed_sum',
        '(group-add (group "group1") (* -1 (series "plain_ts")))'
    )

    df = tsh.group_get(engine, 'mixed_sum')
    assert_df("""
              a    b    c
2015-01-01  1.0  2.0  3.0
2015-01-02  2.0  3.0  4.0
2015-01-03  3.0  4.0  5.0
2015-01-04  4.0  5.0  6.0
2015-01-05  5.0  6.0  7.0
""", df)

    cat = tsh.list_groups(engine)
    assert {
        name: kind for name, kind in cat.items()
        if kind == 'formula'
    } == {
        'group_formula': 'formula',
        'sumgroup': 'formula',
        'mixed_sum': 'formula'
    }

    assert tsh.group_type(engine, 'group_formula') == 'formula'

    assert tsh.group_metadata(engine, 'group_formula') == {
        'index_dtype': '<M8[ns]',
        'index_type': 'datetime64[ns]',
        'tzaware': False,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

    tsh.update_group_metadata(engine, 'group_formula', {'foo': 'bar'})
    assert tsh.group_metadata(engine, 'group_formula') == {
        'index_dtype': '<M8[ns]',
        'index_type': 'datetime64[ns]',
        'tzaware': False,
        'value_dtype': '<f8',
        'value_type': 'float64',
        'foo': 'bar'
    }

    tsh.group_delete(engine, 'group_formula')
    assert not tsh.group_exists(engine, 'group_formula')

    assert tsh.group_metadata(engine, 'group_formula') is None


def test_group_vanilla_formula_history(engine, tsh):
    for idx, idate in enumerate(pd.date_range(start=utcdt(2022, 1, 1),
                                              end=utcdt(2022, 1, 5),
                                              freq='D')):
        df = gengroup(
            n_scenarios=3,
            from_date=idate.date(),
            length=3,
            freq='D',
            seed=10 * idx
        )
        tsh.group_replace(engine, df, 'group_a', 'test', insertion_date=idate)

    for idx, idate in enumerate(pd.date_range(start=utcdt(2022, 1, 1),
                                              end=utcdt(2022, 1, 5),
                                              freq='D')):
        df = gengroup(
            n_scenarios=3,
            from_date=idate.date(),
            length=3,
            freq='D',
            seed=-10 * idx
        )
        tsh.group_replace(engine, df, 'group_b', 'test',
                          insertion_date=idate + timedelta(hours=1))

    formula = '(group-add (group "group_a") (group "group_b"))'

    tsh.register_group_formula(
        engine,
        'history_sum',
        formula,
    )

    idates = tsh.group_insertion_dates(engine, 'history_sum')
    assert idates == [
               pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-01 01:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-02 01:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-03 01:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-04 00:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-04 01:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-05 00:00:00+0000', tz='UTC'),
               pd.Timestamp('2022-01-05 01:00:00+0000', tz='UTC'),
    ]

    hist_a = tsh.group_history(
        engine,
        'group_a',
        from_insertion_date=utcdt(2022, 1, 3),
        to_insertion_date=utcdt(2022, 1, 5)
    )

    hist_b = tsh.group_history(
        engine,
        'group_b',
        from_insertion_date=utcdt(2022, 1, 3),
        to_insertion_date=utcdt(2022, 1, 5)
    )

    hist_sum = tsh.group_history(
        engine,
        'history_sum',
        from_insertion_date=utcdt(2022, 1, 3),
        to_insertion_date=utcdt(2022, 1, 5)
    )

    assert_hist("""
                                         0     1     2
insertion_date            value_date                  
2022-01-03 00:00:00+00:00 2022-01-03  20.0  21.0  22.0
                          2022-01-04  21.0  22.0  23.0
                          2022-01-05  22.0  23.0  24.0
2022-01-04 00:00:00+00:00 2022-01-04  30.0  31.0  32.0
                          2022-01-05  31.0  32.0  33.0
                          2022-01-06  32.0  33.0  34.0
2022-01-05 00:00:00+00:00 2022-01-05  40.0  41.0  42.0
                          2022-01-06  41.0  42.0  43.0
                          2022-01-07  42.0  43.0  44.0    
""", hist_a)

    assert_hist("""
                                         0     1     2
insertion_date            value_date                  
2022-01-03 01:00:00+00:00 2022-01-03 -20.0 -19.0 -18.0
                          2022-01-04 -19.0 -18.0 -17.0
                          2022-01-05 -18.0 -17.0 -16.0
2022-01-04 01:00:00+00:00 2022-01-04 -30.0 -29.0 -28.0
                          2022-01-05 -29.0 -28.0 -27.0
                          2022-01-06 -28.0 -27.0 -26.0
""", hist_b)

    assert_hist("""
                                         0     1     2
insertion_date            value_date                  
2022-01-03 00:00:00+00:00 2022-01-03  11.0  13.0  15.0
                          2022-01-04  13.0  15.0  17.0
2022-01-03 01:00:00+00:00 2022-01-03   0.0   2.0   4.0
                          2022-01-04   2.0   4.0   6.0
                          2022-01-05   4.0   6.0   8.0
2022-01-04 00:00:00+00:00 2022-01-04  11.0  13.0  15.0
                          2022-01-05  13.0  15.0  17.0
2022-01-04 01:00:00+00:00 2022-01-04   0.0   2.0   4.0
                          2022-01-05   2.0   4.0   6.0
                          2022-01-06   4.0   6.0   8.0
2022-01-05 00:00:00+00:00 2022-01-05  11.0  13.0  15.0
                          2022-01-06  13.0  15.0  17.0
""", hist_sum)

    # it seems alright.  One should note the "incoherent" stat when one
    # component is refreshed before the other. It produced a quasi-random state
    # (there are usually no concordance between the scenarios from one run to another)
    # It may call for more developpments....

    # out of bounds:
    assert {} == tsh.group_history(
        engine,
        'history_sum',
        to_insertion_date=utcdt(1978, 7, 15)
    )

    # group does not exists
    assert tsh.group_history(engine, 'missing-group') is None


def test_group_and_series_formula_history(engine, tsh):
    for idx, idate in enumerate(
            pd.date_range(
                start=utcdt(2022, 1, 1),
                end=utcdt(2022, 1, 5),
                freq='D'
            )
    ):
        df = gengroup(
            n_scenarios=3,
            from_date=idate.date(),
            length=3,
            freq='D',
            seed=10 * idx
        )
        tsh.group_replace(engine, df, 'group_c', 'test', insertion_date=idate)

        ts = pd.Series(
            range(3),
            index=pd.date_range(
                start=idate.date(),
                periods=3,
                freq='D')
        ) + idx / 10

        tsh.update(
            engine,
            ts,
            'series_with_group', 'test',
            insertion_date=idate + timedelta(hours=2)
        )

        formula = '(group-add (group "group_c") (series "series_with_group"))'
        tsh.register_group_formula(
            engine,
            'history_mixte',
            formula,
        )

    idates = tsh.group_insertion_dates(engine, 'history_mixte')
    assert [
        pd.Timestamp('2022-01-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-01 02:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 02:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 02:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-04 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-04 02:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-05 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-05 02:00:00+0000', tz='UTC'),
    ] == idates
    # the idate at 00 clcok came from the group, at 02:00 came from the series

    idates = tsh.group_insertion_dates(
        engine,
        'history_mixte',
        from_insertion_date=utcdt(2022, 1, 2),
        to_insertion_date=utcdt(2022, 1, 3)
    )

    assert [
        pd.Timestamp('2022-01-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-02 02:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-01-03 00:00:00+0000', tz='UTC'),
    ] == idates

    hist =  tsh.group_history(engine, 'history_mixte',
                              from_insertion_date=utcdt(2022, 1, 2),
                              to_insertion_date=utcdt(2022, 1, 3))

    assert_hist("""
                                         0     1     2
insertion_date            value_date                  
2022-01-02 00:00:00+00:00 2022-01-02  11.0  12.0  13.0
                          2022-01-03  13.0  14.0  15.0
2022-01-02 02:00:00+00:00 2022-01-02  10.1  11.1  12.1
                          2022-01-03  12.1  13.1  14.1
                          2022-01-04  14.1  15.1  16.1
2022-01-03 00:00:00+00:00 2022-01-03  21.1  22.1  23.1
                          2022-01-04  23.1  24.1  25.1
""", hist)


def test_groups_autotrophic_history(engine, tsh):
    # reset gfuncs
    GroupInterpreter.FUNCS = None

    df1 = gengroup(
        n_scenarios=3,
        from_date=dt(2022, 2, 1),
        length=3,
        freq='D',
        seed=1
    )
    df2 = gengroup(
        n_scenarios=3,
        from_date=dt(2022, 2, 1),
        length=3,
        freq='D',
        seed=-1
    )

    @gfunc('gauto-operator', auto=True)
    def auto_operator(__interpreter__) -> pd.DataFrame:
        revision_date = __interpreter__.getargs['revision_date']
        if revision_date and revision_date < utcdt(2022, 2, 2):
            return df1
        return df2

    @gfinder('gauto-operator')
    def auto_operator_finder(cn, tsh, tree):
        return {'gauto-operator': tree}

    @gmeta('gauto-operator')
    def auto_operator_meta(cn, tsh, tree):
        return {
            f'gauto-operator': {
                'index_dtype': '<M8[ns]',
                'index_type': 'datetime64[ns]',
                'tzaware': False,
                'value_dtype': '<f8',
                'value_type': 'float64'
            }
        }

    @ginsertion_dates('gauto-operator')
    def auto_insertion_dates(
            cn,
            tsh,
            tree,
            from_insertion_date=None,
            to_insertion_date=None,
    ):
        idates = [utcdt(2022, 2, 1), utcdt(2022, 2, 2)]
        if from_insertion_date:
            idates = [idate for idate in idates if idate >= from_insertion_date ]
        if to_insertion_date:
            idates = [idate for idate in idates if idate <= to_insertion_date ]
        return idates

    tsh.register_group_formula(
        engine,
        'auto_group',
        '(gauto-operator)',
    )
    tsh.group_get(engine, 'auto_group')

    # primary group
    for idx, idate in enumerate(pd.date_range(start=utcdt(2022, 2, 1),
                                              end=utcdt(2022, 2, 5),
                                              freq='D')):
        df = gengroup(
            n_scenarios=3,
            from_date=dt(2022, 2, 1),
            length=3,
            freq='D',
            seed=10 * idx
        )
        tsh.group_replace(engine, df, 'group_d', 'test',
                          insertion_date=idate + timedelta(hours=3))

    # 2nd degree formula
    formula = """(group-add (group "auto_group" ) (group "group_d"))"""

    tsh.register_group_formula(
        engine,
        'higher_level',
        formula,
    )
    tsh.group_get(engine, 'higher_level')
    meta = tsh.group_metadata(engine, 'higher_level')
    assert meta == {
        'tzaware': False,
        'index_type': 'datetime64[ns]',
        'value_type': 'float64',
        'index_dtype': '<M8[ns]',
        'value_dtype': '<f8'
    }

    idates = tsh.group_insertion_dates(engine, 'higher_level')

    assert [
        pd.Timestamp('2022-02-01 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-01 03:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-02 03:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-03 03:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-04 03:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-05 03:00:00+0000', tz='UTC'),
    ] == idates
    # the idates at 3 o'ckock came from the primary
    # the ones at 00h came from the autotrophic

    idates = tsh.group_insertion_dates(
        engine,
        'higher_level',
        from_insertion_date=utcdt(2022, 2, 2),
        to_insertion_date=utcdt(2022, 2, 4)
    )

    assert [
        pd.Timestamp('2022-02-02 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-02 03:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-02-03 03:00:00+0000', tz='UTC'),
    ] == idates

    hist = tsh.group_history(engine, 'higher_level',
                           from_insertion_date=utcdt(2022, 2, 2),
                           to_insertion_date=utcdt(2022, 2, 3))

    assert_hist("""
                                         0     1     2
insertion_date            value_date                  
2022-02-02 00:00:00+00:00 2022-02-01  -1.0   1.0   3.0
                          2022-02-02   1.0   3.0   5.0
                          2022-02-03   3.0   5.0   7.0
2022-02-02 03:00:00+00:00 2022-02-01   9.0  11.0  13.0
                          2022-02-02  11.0  13.0  15.0
                          2022-02-03  13.0  15.0  17.0
""", hist)


def test_group_bound_formula(engine, tsh):
    temp = pd.Series(
        [12, 13, 14],
        index=pd.date_range(utcdt(2021, 1, 1), freq='D', periods=3)
    )
    wind = pd.Series(
        [.1, .1, .1],
        index=pd.date_range(utcdt(2021, 1, 1), freq='D', periods=3)
    )

    tsh.update(engine, temp, 'base-temp', 'Babar')
    tsh.update(engine, wind, 'base-wind', 'Celeste')

    tsh.register_formula(
        engine,
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
    tsh.group_replace(
        engine,
        df1,
        'temp-ens',
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
    tsh.group_replace(
        engine,
        df2,
        'wind-ens',
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

    tsh.register_formula_bindings(
        engine,
        'hijacking',
        'hijacked',
        binding
    )

    ts = tsh.get(engine, 'hijacked')
    assert_df("""
2021-01-01 00:00:00+00:00    12.1
2021-01-02 00:00:00+00:00    13.1
2021-01-03 00:00:00+00:00    14.1
""", ts)

    df = tsh.group_get(engine, 'hijacking')
    assert_df("""
                             0    1
2021-01-01 00:00:00+00:00  1.0  3.0
2021-01-02 00:00:00+00:00  3.0  5.0
2021-01-03 00:00:00+00:00  5.0  7.0
""", df)

    assert tsh.group_exists(engine, 'hijacking')
    assert tsh.group_type(engine, 'hijacking') == 'bound'

    cat = tsh.list_groups(engine)
    assert {
        name: kind for name, kind in cat.items()
        if kind == 'bound'
    } == {
        'hijacking': 'bound'
    }

    assert tsh.group_metadata(engine, 'hijacking') == {
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }
    tsh.update_group_metadata(engine, 'hijacking', {'foo': 'bar'})
    assert tsh.group_metadata(engine, 'hijacking') == {
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64',
        'foo': 'bar'
    }

    tsh.group_delete(engine, 'hijacking')
    assert not tsh.group_exists(engine, 'hijacking')

    assert tsh.group_metadata(engine, 'hijacking') is None


def test_group_bound_history(engine, tsh):
    # formula with 3 series (a, b, c)
    # hijacked by two groups a and b

    # series
    for idx in range(10):
        idate = utcdt(2022, 4, idx+1)
        ts = pd.Series(
            [idx] * 5,
            index=pd.date_range(idate, periods=5, freq='D')
        )
        for sn in ('series-a', 'series-b', 'series-c', 'irrelevant-series'):
            tsh.update(engine, ts, sn, 'test', insertion_date=idate)

    # groups
    for idx in range(5):
        idate = utcdt(2022, 4, idx+1)
        df = gengroup(3, from_date=idate, length=3, freq='D', seed=idx)
        tsh.group_replace(engine, df, 'group-a', 'test', insertion_date=idate)

    tsh.group_replace(
        engine,
        gengroup(3, from_date=idate, length=3, freq='D', seed=idx),
        'irrelevant-group',
        'test',
        insertion_date=utcdt(2023, 1, 1)
    )

    for idx in range(5):
        idate = utcdt(2022, 4, idx+3)
        df = gengroup(3, from_date=idate, length=3, freq='D', seed=idx)
        tsh.group_replace(
            engine,
            df + 3.14,
            'group-b',
            'test',
            insertion_date=idate + timedelta(hours=1)
        )

    tsh.register_formula(
        engine,
        'formula_series_history',
        '(add (series "series-a") (series "series-b") (series "series-c"))'
    )

    binding = pd.DataFrame(
        [
            ['series-a', 'group-a', 'history'],
            ['series-b', 'group-b', 'history'],
            ['irrelevant-series', 'irrelevant-group', 'history'],
        ],
        columns=('series', 'group', 'family')
    )

    tsh.register_formula_bindings(
        engine,
        'formula_group_history',
        'formula_series_history',
        binding
    )

    idates = tsh.group_insertion_dates(engine, 'formula_group_history')

    assert idates == [
        pd.Timestamp('2022-04-03 01:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-04-04 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-04-04 01:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-04-05 00:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-04-05 01:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-04-06 01:00:00+0000', tz='UTC'),
        pd.Timestamp('2022-04-07 01:00:00+0000', tz='UTC'),
    ]

    rdate = utcdt(2022, 4, 5, 1)
    group_past = tsh.group_get(
        engine,
        'formula_group_history',
        revision_date=rdate
    )

    hist = tsh.group_history(
        engine,
        'formula_group_history'
    )

    assert_df("""
                               0      1      2
2022-04-05 00:00:00+00:00  13.14  15.14  17.14
2022-04-06 00:00:00+00:00  15.14  17.14  19.14
2022-04-07 00:00:00+00:00  17.14  19.14  21.14
""", group_past)

    # coherence between .get() with revdate and history:
    assert hist[rdate].equals(group_past)
    # check: state of each component and its sum
    ts_c_past = tsh.get(engine, 'series-c', revision_date=rdate)
    df_a_past = tsh.group_get(engine, 'group-a', revision_date=rdate)
    df_b_past = tsh.group_get(engine, 'group-b', revision_date=rdate)
    assert group_past.equals(
        df_b_past.add(ts_c_past, axis=0).dropna() + df_a_past
    )
    group_past = tsh.group_get(
        engine,
        'formula_group_history',
        revision_date=rdate,
        from_value_date=utcdt(2022, 4, 5, 12),
        to_value_date=utcdt(2022, 4, 6, 12)
    )

    assert_df("""
                               0      1      2
2022-04-06 00:00:00+00:00  15.14  17.14  19.14
""", group_past)


def test_for_group_optimization(engine, tsh):
    """
    In here we build a hijacked dependency tree
    to test an optimization of hijacking:
    * The tree:
    A-+-B--C
      +-G--SG
    C and SG are primary series
    G will be hijacked by a group
    SG is a series that should not be evaluated during the hijacking
    * The optim:
    During the hijacking, the formula tree is completely evaluated for each scenario
    which is obviously very redundant.
    In this case, B and C should only be evaluated once: this is the point of the
    upcoming optim
    """

    # Bulding the tree dependency
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2022, 1, 1), periods=3, freq='D')
    )

    df = gengroup(
        n_scenarios=3,
        from_date=dt(2022, 1, 1),
        length=3,
        freq='D',
        seed=2
    )

    # primaries:
    tsh.update(engine, ts, 'ts-c', 'test')
    tsh.update(engine, ts, 'ts-sg', 'test')

    # group:
    tsh.group_replace(engine, df, 'group-o', 'test')

    # formulas:
    tsh.register_formula(engine, 'ts-b', '(* 2 (series "ts-c"))')
    tsh.register_formula(engine, 'ts-g', '(* 3 (series "ts-sg"))')
    tsh.register_formula(engine, 'ts-a', '(add (series "ts-b") (series "ts-g"))')

    assert_df("""
2022-01-01     5.0
2022-01-02    10.0
2022-01-03    15.0
""", tsh.get(engine, 'ts-a'))

    # hijacking
    binding = pd.DataFrame(
        [['ts-g', 'group-o', 'optim']],
        columns = ['series', 'group', 'family']
    )

    tsh.register_formula_bindings(
        engine,
        'group-to-optim',
        'ts-a',
        binding=binding,
    )

    df = tsh.group_get(engine, 'group-to-optim')
    assert_df("""
               0     1     2
2022-01-01   4.0   5.0   6.0
2022-01-02   7.0   8.0   9.0
2022-01-03  10.0  11.0  12.0
""", df)

    # The ts-c series is loaded 3 times
    # stdout:
    """
Starting hijacking...
Hijacking: evaluate ts-b
Hijacking: load from cache ts-b
Hijacking: load from cache ts-b

    """


def test_bound_formula_group_crash(engine, tsh):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2023, 1, 1), periods=3, freq='D')
    )

    tsh.update(
        engine,
        ts,
        'crash-base',
        'Babbar',
        insertion_date=utcdt(2023, 1, 1)
    )

    tsh.register_formula(
        engine,
        'crash-bottom',
        '(slice (series "crash-base") #:fromdate (date "2022-1-1"))'
    )
    tsh.register_formula(
        engine,
        'crash-middle',
        '(series "crash-bottom")'
    )
    tsh.register_formula(
        engine,
        'crash-top',
        '(priority (series "crash-base") '
        '          (series "crash-middle"))'
    )

    df = gengroup(
        n_scenarios=3,
        from_date=dt(2022, 1, 1),
        length=3,
        freq='D',
        seed=2
    )
    tsh.group_replace(engine, df, 'some-group', 'test')

    binding = pd.DataFrame(
        [
            ['crash-base', 'some-group', 'crash'],
        ],
        columns=('series', 'group', 'family')
    )

    tsh.register_formula_bindings(
        engine,
        'crash-group',
        'crash-top',
        binding=binding,
    )

    # we do not go into a recursion error any longer
    tsh.group_get(engine, 'crash-group')


# migration helper
def test_migrate_to_round(engine, tsh):
    f1 = lisp.parse('(* 2 (trig.cos (series "trig-series")))')
    assert rewrite_trig_formula(
        f1
    ) == f1

    f2 = lisp.parse('(* 2 (trig.cos (series "trig-series") #:decimals 3))')
    assert rewrite_trig_formula(
        f2
    ) == lisp.parse('(* 2 (round (trig.cos (series "trig-series")) #:decimals 3))')