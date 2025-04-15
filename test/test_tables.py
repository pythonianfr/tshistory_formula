import pytest

from tshistory.testutil import tables

from tshistory_formula.schema import formula_schema
from tshistory_formula.tsio import timeseries


@pytest.fixture(scope='session')
def pure(engine):
    formula_schema('pure').create(engine)
    yield timeseries('pure')


def test_tables(engine, pure):
    with engine.begin() as cn:
        assert tables(cn) == [
            ('pure', 'basket'),
            ('pure', 'dependent'),
            ('pure', 'form_history'),
            ('pure', 'gr_oldmeta'),
            ('pure', 'group_binding'),
            ('pure', 'group_registry'),
            ('pure', 'group_series_map'),
            ('pure', 'groupmap'),
            ('pure', 'registry'),
            ('pure', 'revision_metadata'),
            ('pure', 'tree'),
            ('pure', 'tree_series_map'),
            ('pure', 'ts_oldmeta'),
            ('pure-formula-patch', 'registry'),
            ('pure-formula-patch', 'revision_metadata'),
            ('pure-formula-patch-kvstore', 'kvstore'),
            ('pure-formula-patch-kvstore', 'things'),
            ('pure-formula-patch-kvstore', 'version'),
            ('pure-formula-patch-kvstore', 'vkvstore'),
            ('pure-kvstore', 'kvstore'),
            ('pure-kvstore', 'things'),
            ('pure-kvstore', 'version'),
            ('pure-kvstore', 'vkvstore'),
            ('pure.group', 'registry'),
            ('pure.group', 'revision_metadata'),
            ('pure.group-kvstore', 'kvstore'),
            ('pure.group-kvstore', 'things'),
            ('pure.group-kvstore', 'version'),
            ('pure.group-kvstore', 'vkvstore')
        ]
