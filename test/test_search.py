from tshistory_formula import search


def _serialize_roundtrip(searchobj):
    return search.query.fromexpr(searchobj.expr()).expr() == searchobj.expr()


def test_search():
    s0 = search.byformulacontents('integration')
    assert s0.expr() == '(by.formulacontents "integration")'
    assert _serialize_roundtrip(s0)

    s1 = search.isformula()
    assert s1.expr() == '(by.formula)'
    assert _serialize_roundtrip(s1)
