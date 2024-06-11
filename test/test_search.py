from datetime import datetime as dt

import pandas as pd
import pytest

from tshistory_formula import search
from tshistory.search import query
from tshistory_formula.helper import replace_findseries


def _serialize_roundtrip(searchobj):
    return search.query.fromexpr(searchobj.expr()).expr() == searchobj.expr()


def test_search():
    s0 = search.byformulacontents('integration')
    assert s0.expr() == '(by.formulacontents "integration")'
    assert _serialize_roundtrip(s0)

    s1 = search.isformula()
    assert s1.expr() == '(by.formula)'
    assert _serialize_roundtrip(s1)


def test_replace_findseries(engine, tsh):
    # tzware
    ts = pd.Series([1], [pd.Timestamp('2024-01-01', tz='UTC'),])
    tsh.update(engine, ts, 'to-replace-0', 'test')
    tsh.update(engine, ts, 'to-replace-1', 'test')
    tsh.update(engine, ts, 'other-one', 'test')

    # naive
    ts = pd.Series([1], [pd.Timestamp('2024-01-01'),])
    tsh.update(engine, ts, 'to-replace-0-naive', 'test')
    tsh.update(engine, ts, 'to-replace-1-naive', 'test')
    tsh.update(engine, ts, 'other-one-naive', 'test')

    formula = """
    (priority
        (add (findseries (by.name "to-replace")))
        (series "other-one"))
    """
    tsh.register_formula(engine, 'swithcheroo', formula)

    formula_naive = """
    (priority
        (add (findseries (by.name "to-replace") #:naive #t))
        (series "other-one-naive"))
    """
    tsh.register_formula(engine, 'swithcheroo-naive', formula_naive)

    # tz-aware
    substitued = replace_findseries(engine, tsh, formula)
    assert substitued == (
        '(priority (add (series "to-replace-0")'
        ' (series "to-replace-1"))'
        ' (series "other-one"))'
    )
    assert tsh.get(engine, 'swithcheroo').equals(
        tsh.eval_formula(engine, substitued)
    )

    # naive
    substitued = replace_findseries(engine, tsh, formula_naive)
    assert substitued == (
        '(priority (add (series "to-replace-0-naive")'
        ' (series "to-replace-1-naive"))'
        ' (series "other-one-naive"))'
    )
    assert tsh.get(engine, 'swithcheroo-naive').equals(
        tsh.eval_formula(engine, substitued)
    )

    #fill

    formula = """
    (add (findseries (by.name "to-replace") #:fill "ffill"))
    """
    tsh.register_formula(engine, 'find-and-fill', formula)

    substitued = replace_findseries(engine, tsh, formula)
    assert substitued == """(add (series "to-replace-0" #:fill "ffill") (series "to-replace-1" #:fill "ffill"))"""
    assert tsh.get(engine, 'find-and-fill').equals(
        tsh.eval_formula(engine, substitued)
    )

    # empty return

    formula = """
    (priority
        (add (findseries (by.name "no-such-series")))
        (series "other-one"))
    """
    tsh.register_formula(engine, 'degenerate-find', formula)
    substitued = replace_findseries(engine, tsh, formula)
    assert substitued == """(priority (add) (series "other-one"))"""
    assert tsh.get(engine, 'degenerate-find').equals(
        tsh.eval_formula(engine, substitued)
    )


def test_find_from_expr(engine, tsh):
    expr = '(by.formulacontents "whatever")'
    tsh.find(engine, query.fromexpr(expr))

    expr = '(by.basket "no-basket")'
    with pytest.raises(KeyError):
        tsh.find(engine, query.fromexpr(expr))

    expr = '(by.value "whatver" ">" 23)'
    with pytest.raises(KeyError):
        tsh.find(engine, query.fromexpr(expr))


def test_basket_by_value(engine, tsx):
    basket_definition = '(<  "whatever" 37)'
    # with pytest.raises(Exception) as error:
    tsx.register_basket('basket-by-value', basket_definition)
    # assert str(error.value) == "'by.value"


def test_expanded_and_find(engine, tsh):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(dt(2022, 1, 1), periods=3, freq='D')
    )
    tsh.update(engine, ts, 'level-find-base-a', 'Babar')
    tsh.update(engine, ts, 'level-find-base-b', 'Babar')
    tsh.update(engine, ts, 'level-find-series', 'Babar')

    tsh.register_formula(
        engine,
        'level-find-0',
        '(add (findseries (by.name "find-base") #:naive #t) (series "level-find-series"))')
    tsh.register_formula(
        engine,
        'level-find-1',
        '(+ 1 (series "level-find-0"))'
    )

    assert tsh.depth(engine, 'level-find-0') == 1
    assert tsh.depth(engine, 'level-find-1') == 2

    exp = tsh.expanded_formula(engine, 'level-find-1', display=False)
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (+ 1 (add (series "level-find-base-a")'
        ' (series "level-find-base-b")'
        ' (series "level-find-series"))))'
    )
    exp = tsh.expanded_formula(engine, 'level-find-1', level=0, display=False)
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil '
        '(+ 1 (series "level-find-0")))'
    )
    exp = tsh.expanded_formula(engine, 'level-find-1', level=1, display=False)
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil '
        '(+ 1 (add (findseries (by.name "find-base") #:naive #t) '
        '(series "level-find-series"))))'
    )
    exp = tsh.expanded_formula(engine, 'level-find-1', level=2, display=False)
    assert exp == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (+ 1 (add (series "level-find-base-a")'
        ' (series "level-find-base-b")'
        ' (series "level-find-series"))))'
    )
    exp3 = tsh.expanded_formula(engine, 'level-find-1', level=3, display=False)
    assert exp3 == exp


def test_nested_find(tsh, engine):
    ts = pd.Series(
        [1, 2, 3],
        index=pd.date_range(
            dt(2022, 1, 1),
            periods=3,
            freq='D',
            tz='CET',
        )
    )
    tsh.update(engine, ts, 'nested-find-base-a', 'Babar')
    tsh.update(engine, ts, 'nested-find-base-b', 'Babar')

    formula = '(add (series "nested-find-base-a") (series "nested-find-base-b"))'
    tsh.register_formula(engine, 'nested-find-1', formula)
    formula = '(add (findseries (by.name "nested-find-1")))'
    tsh.register_formula(engine, 'nested-find-2', formula)

    assert tsh.depth(engine, 'nested-find-2') == 2
    assert tsh.expanded_formula(engine, 'nested-find-2') == (
        '(add (add (series "nested-find-base-a") (series "nested-find-base-b")))'
    )


def test_find_dependents(engine, tsh):
    ts = pd.Series(
        [1, 2],
        index = pd.date_range(
            start=dt(2022, 1, 1),
            periods=2,
            freq='D',
            tz='CET',
        )
    )

    tsh.update(engine, ts, 'find-base-dep', 'test')
    tsh.register_formula(
        engine,
        'find-dep-level-1',
        '(series "find-base-dep")'
    )

    tsh.register_formula(
        engine,
        'find-dep-level-2',
        '(add (findseries (by.name "find-dep-level-1")))'
    )

    tsh.register_formula(
        engine,
        'find-dep-level-3',
        '(series "find-dep-level-2")'
    )

    tsh.register_formula(
        engine,
        'find-dep-level-4',
        '(add (findseries (by.name "find-dep-level-3")))'
    )

    assert tsh.dependents(engine, 'find-base-dep') == []
    assert tsh.dependents(engine, 'find-dep-level-1') == [
        'find-dep-level-2', 'find-dep-level-3', 'find-dep-level-4'
    ]
    assert tsh.dependents(engine, 'find-dep-level-2') == [
        'find-dep-level-3', 'find-dep-level-4'
    ]
    assert tsh.dependents(engine, 'find-dep-level-3') == [
        'find-dep-level-4'
    ]
    assert tsh.dependents(engine, 'find-dep-level-4') == []
