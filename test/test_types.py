import json
import inspect
import itertools
import typing

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
    Number,
    Packed,
    sametype,
    typecheck,
    typename
)
from tshistory_formula.vocabulary import (
    FILL_METHODS,
    TIMEZONES
)


NONETYPE = type(None)


def test_dtypes():
    index = pd.date_range(
        pd.Timestamp('2021-1-1'),
        periods=3,
        freq='h'
    )
    assert index.dtype.str == '<M8[ns]'
    index2 = pd.date_range(
        pd.Timestamp('2021-1-1', tz='UTC'),
        periods=3,
        freq='h'
    )
    assert index2.dtype.str == '|M8[ns]'


def test_function_types():
    f = FUNCS['findseries']
    types = function_types(f)
    assert types == {
        'fill': f'Default[Union[{typename(FILL_METHODS)}, Number]=None]',
        'naive': 'Default[bool=False]',
        'q': 'query',
        'return': 'List[Series]'
    }

    f = FUNCS['add']
    types = function_types(f)
    assert types == {
        'return': 'Series',
        'serieslist': 'Packed[Series]'
    }

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
        'fill': f'Default[Union[{typename(FILL_METHODS)}, Number]=None]',
        'keepnans': 'Default[bool=False]',
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
        'tz': f'Default[{typename(TIMEZONES)}="UTC"]'
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


@pytest.mark.golden_test("data/test_operators_types.yml")
def test_operators_types(golden):
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
         'max', 'min', 'mul', 'naive', 'now',
         'options', 'priority', 'resample', 'rolling', 'round', 'row-max',
         'row-mean', 'row-min', 'series', 'serieslist',
         'shifted', 'slice', 'start-of-month',
         'std', 'sub', 'time-shifted',
         'trig.cos', 'trig.arccos', 'trig.sin', 'trig.arcsin',
         'trig.tan', 'trig.arctan', 'trig.row-arctan2',
         'tzaware-tstamp')
    )
    types = {
        name: ftype
        for name, ftype in json.loads(jsontypes()).items()
        if name in opnames
    }
    # Update output with :
    #
    #   $ rm test/data/test_operators_types.yml
    #   $ touch test/data/test_operators_types.yml
    #   $ pytest --update-goldens -k operators_types
    #
    assert types == golden.out["output_spec"]


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

    assert err.value.args[0] == "'toto' not a Number"


def test_failing_kw(engine, tsh):
    expr = '(+ 1 (series 42))'
    i = Interpreter(engine, tsh, {})
    with pytest.raises(TypeError) as err:
        typecheck(lisp.parse(expr), i.env)

    assert err.value.args[0] == "42 not a seriesname"


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
