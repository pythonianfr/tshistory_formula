import pandas as pd
import pytest

from tshistory_formula import search
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
