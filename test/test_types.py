import json
import inspect
import itertools
import typing
from numbers import Number

import pytest
import pandas as pd
from psyl import lisp

from tshistory_formula.interpreter import (
    Interpreter,
    jsontypes
)
from tshistory_formula.registry import func, FUNCS
from tshistory_formula.helper import seriesname
from tshistory_formula.types import (
    isoftype,
    function_types,
    findtype,
    Packed,
    sametype,
    typecheck
)


NONETYPE = type(None)


def test_dtypes():
    index = pd.date_range(
        pd.Timestamp('2021-1-1'),
        periods=3,
        freq='H'
    )
    assert index.dtype.str == '<M8[ns]'
    index2 = pd.date_range(
        pd.Timestamp('2021-1-1', tz='UTC'),
        periods=3,
        freq='H'
    )
    assert index2.dtype.str == '|M8[ns]'


def test_function_types():
    f = FUNCS['shifted']
    types = function_types(f)
    assert types == {
        'date': 'Timestamp',
        'days': 'Default[int=0]',
        'hours': 'Default[int=0]',
        'minutes': 'Default[int=0]',
        'months': 'Default[int=0]',
        'return': 'Timestamp',
        'weeks': 'Default[int=0]',
        'years': 'Default[int=0]'
    }

    f = FUNCS['series']
    types = function_types(f)
    assert types == {
        'fill': 'Default[Union[str, Number]=None]',
        'limit': 'Default[int=None]',
        'name': 'seriesname',
        'return': 'Series',
        'weight': 'Default[Number=None]'
    }

    f = FUNCS['date']
    types = function_types(f)
    assert types == {
        'return': 'Timestamp',
        'strdate': 'str',
        'tz': 'Default[str="UTC"]'
    }

    f = FUNCS['by.name']
    types = function_types(f)
    assert types == {
        'namequery': 'str',
        'return': 'query'
    }


def test_sametype():
    types = (str, int, float, pd.Series)
    for t1, t2 in itertools.product(types, types):
        if t1 == t2:
            continue
        assert not sametype(t1, t2)

    for t in types:
        assert sametype(t, t)
        assert sametype(typing.Union[NONETYPE, t], t)
        assert sametype(t, typing.Union[NONETYPE, t])
        assert sametype(
            typing.Union[NONETYPE, t],
            typing.Union[NONETYPE, t]
        )

    assert sametype(typing.Union[NONETYPE, int, pd.Series], int)
    assert sametype(typing.Union[NONETYPE, int, pd.Series], pd.Series)
    assert sametype(typing.Union[NONETYPE, int, pd.Series], NONETYPE)

    assert sametype(int, typing.Union[NONETYPE, int, pd.Series])
    assert sametype(pd.Series, typing.Union[NONETYPE, int, pd.Series])
    assert sametype(NONETYPE, typing.Union[NONETYPE, int, pd.Series])

    assert sametype(int, Number)
    assert sametype(Number, int)
    assert sametype(int, typing.Union[Number, pd.Series])
    assert sametype(Number, typing.Union[int, pd.Series])

    assert sametype(object, str)
    assert sametype(seriesname, str)

    assert sametype(typing.Tuple[object], typing.Tuple[object])
    assert sametype(typing.Tuple[object], typing.Tuple[str])
    assert not sametype(typing.Tuple[str], typing.Tuple[object])


def test_isoftype():
    assert isoftype(int, 1)
    assert isoftype(Number, 1)
    assert not isoftype(str, 1)
    assert isoftype(typing.Union[NONETYPE, int], 1)
    assert isoftype(typing.Union[NONETYPE, Number], 1)


def test_findtype():
    def foo(__nope__, a:int, *b:str, kw1:str = None, kw2:int = 42) -> typing.List[str]:
        pass

    sig = inspect.signature(foo)

    atype = findtype(sig, 1)
    assert atype is int
    btype = findtype(sig, 2)
    assert btype == Packed[str]


def test_operators_types():
    # prune the types registered from other modules/plugins
    # we want to only show the ones provided by the current package
    opnames = set(
        ('*', '**', '+', '/',
         '<', # '<=', '<>', '==', '>', '>=',
         'abs', 'add', 'asof',
         'by.and', 'by.or',
         'by.name', 'by.metaitem', 'by.metakey', 'by.value',
         'clip', 'constant', 'cumsum', 'div',
         'end-of-month',
         'findseries',
         'holidays',
         'integration',
         'max', 'min', 'mul', 'naive',
         'options', 'priority', 'resample', 'rolling', 'round', 'row-max',
         'row-mean', 'row-min', 'series', 'serieslist',
         'shifted', 'slice', 'start-of-month',
         'std', 'sub', 'today', 'time-shifted',
         'trig.cos', 'trig.arccos', 'trig.sin', 'trig.arcsin',
         'trig.tan', 'trig.arctan', 'trig.row-arctan2',
         'tzaware-tstamp')
    )
    types = {
        name: ftype
        for name, ftype in json.loads(jsontypes()).items()
        if name in opnames
    }
    assert {
        '*': {'num': 'Number',
              'num_or_series': 'Union[Number, Series]',
              'return': 'Union[Number, Series]'},
        '**': {'num': 'Number',
               'return': 'Series',
               'series': 'Series'},
        '+': {'num': 'Number',
              'num_or_series': 'Union[Number, Series]',
              'return': 'Union[Number, Series]'},
        '/': {'num_or_series': 'Union[Number, Series]',
              'num': 'Number',
              'return': 'Union[Number, Series]'},
        '<': {'false_value': 'Default[Number=0]',
              'num_or_series': 'Union[Number, Series]',
              'return': 'Series',
              'series': 'Series',
              'true_value': 'Default[Number=1]'},
        'abs': {'return': 'Series', 'series': 'Series'},
        'add': {'return': 'Series', 'serieslist': 'Series'},
        'asof': {'return': 'Series',
                 'revision_date': 'Timestamp',
                 'series': 'Series'},
        'by.and': {'queries': 'query', 'return': 'query'},
        'by.metaitem': {'key': 'str', 'return': 'query', 'value': 'Union[str, Number]'},
        'by.metakey': {'keyquery': 'str', 'return': 'query'},
        'by.name': {'namequery': 'str', 'return': 'query'},
        'by.or': {'queries': 'query', 'return': 'query'},
        'by.value': {'key': 'str',
                     'operator': 'str',
                     'return': 'query',
                     'value': 'Union[str, Number]'},
        'clip': {'max': 'Default[Number=None]',
                 'min': 'Default[Number=None]',
                 'replacemax': 'Default[bool=False]',
                 'replacemin': 'Default[bool=False]',
                 'return': 'Series',
                 'series': 'Series'},
        'constant': {'freq': 'str',
                     'fromdate': 'Timestamp',
                     'return': 'Series',
                     'revdate': 'Timestamp',
                     'todate': 'Timestamp',
                     'value': 'Number'},
        'cumsum': {'return': 'Series', 'series': 'Series'},
        'div': {'return': 'Series', 's1': 'Series', 's2': 'Series'},
        'findseries': {'return': 'Series',
                       'naive': 'Default[bool=False]',
                       'q': 'query',
                       'fill': 'Default[Union[str, Number]=None]'},
        'end-of-month': {'date': 'Timestamp', 'return': 'Timestamp'},
        'holidays':{'country': 'str',
                    'naive': 'Default[bool=False]',
                    'return': 'Series'},
        'integration': {'fill': 'Default[bool=False]',
                        'flow_name': 'str',
                        'return': 'Series',
                        'stock_name': 'str'},
        'mul': {'return': 'Series', 'serieslist': 'Series'},
        'naive': {'return': 'Series', 'series': 'Series', 'tzone': 'str'},
        'options': {'fill': 'Default[Union[str, Number]=None]',
                    'limit': 'Default[int=None]',
                    'return': 'Series',
                    'series': 'Series',
                    'weight': 'Default[Number=None]'},
        'priority': {'return': 'Series', 'serieslist': 'Series'},
        'resample': {'freq': 'str',
                     'method': 'Default[str="mean"]',
                     'return': 'Series',
                     'series': 'Series'},
        'round': {'decimals': 'Default[Number=0]',
                  'return': 'Series',
                  'series': 'Series'},
        'rolling': {'method': 'Default[str="mean"]',
                    'return': 'Series',
                    'series': 'Series',
                    'window': 'int'},
        'row-max': {'return': 'Series',
                    'serieslist': 'Series',
                    'skipna': 'Default[bool=True]'},
        'row-mean': {'return': 'Series',
                     'serieslist': 'Series',
                     'skipna': 'Default[bool=True]'},
        'row-min': {'return': 'Series',
                    'serieslist': 'Series',
                    'skipna': 'Default[bool=True]'},
        'series': {'fill': 'Default[Union[str, Number]=None]',
                   'limit': 'Default[int=None]',
                   'name': 'seriesname',
                   'return': 'Series',
                   'weight': 'Default[Number=None]'},
        'shifted': {'date': 'Timestamp',
                    'days': 'Default[int=0]',
                    'hours': 'Default[int=0]',
                    'minutes': 'Default[int=0]',
                    'months': 'Default[int=0]',
                    'return': 'Timestamp',
                    'weeks': 'Default[int=0]',
                    'years': 'Default[int=0]'},
        'slice': {'fromdate': 'Default[Timestamp=None]',
                  'return': 'Series',
                  'series': 'Series',
                  'todate': 'Default[Timestamp=None]'},
        'start-of-month': {'date': 'Timestamp', 'return': 'Timestamp'},
        'std': {'return': 'Series',
                'serieslist': 'Series',
                'skipna': 'Default[bool=True]'},
        'sub': {'return': 'Series',
                'series1': 'Series',
                'series2': 'Series'},
        'time-shifted': {'days': 'Default[int=0]',
                         'hours': 'Default[int=0]',
                         'minutes': 'Default[int=0]',
                         'return': 'Series',
                         'series': 'Series',
                         'weeks': 'Default[int=0]'},
        'today': {'naive': 'Default[bool=False]',
                  'return': 'Timestamp',
                  'tz': 'Default[str=None]'},
        'trig.arccos': {'return': 'Series', 'series': 'Series'},
        'trig.arcsin': {'return': 'Series', 'series': 'Series'},
        'trig.arctan': {'return': 'Series', 'series': 'Series'},
        'trig.cos': {'return': 'Series', 'series': 'Series'},
        'trig.row-arctan2': {'return': 'Series',
                             'series1': 'Series',
                             'series2': 'Series'},
        'trig.sin': {'return': 'Series', 'series': 'Series'},
        'trig.tan': {'return': 'Series', 'series': 'Series'}
    } == types


def test_operators_is_typed():
    def foo(x, *y, z=42):
        return x

    with pytest.raises(TypeError) as err:
        func('foo')(foo)
    assert err.value.args[0] == (
        'operator `foo` has type issues: arguments x, y, z are untyped, '
        'return type is not provided'
    )


def test_basic_typecheck():
    def plus(a: int, b: int) -> int:
        return a + b

    env = lisp.Env({'+': plus})
    expr = ('(+ 3 4)')
    typecheck(lisp.parse(expr), env=env)

    expr = ('(+ 3 "hello")')
    with pytest.raises(TypeError):
        typecheck(lisp.parse(expr), env=env)

    def mul(a: int, b: int) -> int:
        return a * b

    env = lisp.Env({'+': plus, '*': mul})
    expr = ('(* 2 (+ 3 "hello"))')
    with pytest.raises(TypeError):
        typecheck(lisp.parse(expr), env=env)


def test_complex_typecheck(engine, tsh):
    expr = ('(add (series "types-a") '
            '     (priority (series "types-a") '
            '               (* 2 (series "types-b"))))'
    )

    i = Interpreter(engine, tsh, {})
    rtype = typecheck(lisp.parse(expr), i.env)
    assert rtype.__name__ == 'Series'


def test_failing_arg(engine, tsh):
    expr = ('(add (series "types-a") '
            '     (priority (series "types-a") '
            '               (* "toto" (series "types-b"))))'
    )

    i = Interpreter(engine, tsh, {})
    with pytest.raises(TypeError) as err:
        typecheck(lisp.parse(expr), i.env)

    assert err.value.args[0] == "'toto' not of <class 'numbers.Number'>"


def test_failing_kw(engine, tsh):
    expr = '(+ 1 (series 42))'
    i = Interpreter(engine, tsh, {})
    with pytest.raises(TypeError) as err:
        typecheck(lisp.parse(expr), i.env)

    assert err.value.args[0] == "42 not of <class 'tshistory_formula.helper.seriesname'>"


def test_kw_subexpr(engine, tsh):
    expr = '(+ 1 (series "types-a" #:weight (+ 1 2)))'
    i = Interpreter(engine, tsh, {})
    typecheck(lisp.parse(expr), i.env)


def test_narrowing(engine, tsh):
    i = Interpreter(engine, tsh, {})
    for expr in (
        '(+ 2 (series "foo"))',
        '(* 2 (series "foo"))',
        '(/ (series "foo") 2)'):
        rtype = typecheck(lisp.parse(expr), i.env)
        assert rtype == pd.Series
