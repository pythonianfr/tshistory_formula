import pytest

from tshistory.testutil import tables

from tshistory_formula.schema import formula_schema
from tshistory_formula.tsio import timeseries


@pytest.fixture(scope='session')
def pure(engine):
    formula_schema('pure').create(engine)
    yield timeseries('pure')


def test_tables(engine, pure):
    assert tables(engine) == [
        ('pure', 'basket'),
        ('pure', 'dependent'),
        ('pure', 'group_registry'),
        ('pure', 'groupmap'),
        ('pure', 'registry'),
        ('pure', 'revision_metadata'),
        ('pure-formula-patch', 'basket'),
        ('pure-formula-patch', 'group_registry'),
        ('pure-formula-patch', 'groupmap'),
        ('pure-formula-patch', 'registry'),
        ('pure-formula-patch', 'revision_metadata'),
        ('pure-formula-patch-kvstore', 'kvstore'),
        ('pure-formula-patch-kvstore', 'things'),
        ('pure-formula-patch-kvstore', 'version'),
        ('pure-formula-patch-kvstore', 'vkvstore'),
        ('pure-formula-patch.group', 'basket'),
        ('pure-formula-patch.group', 'registry'),
        ('pure-formula-patch.group', 'revision_metadata'),
        ('pure-formula-patch.group-kvstore', 'kvstore'),
        ('pure-formula-patch.group-kvstore', 'things'),
        ('pure-formula-patch.group-kvstore', 'version'),
        ('pure-formula-patch.group-kvstore', 'vkvstore'),
        ('pure-kvstore', 'kvstore'),
        ('pure-kvstore', 'things'),
        ('pure-kvstore', 'version'),
        ('pure-kvstore', 'vkvstore'),
        ('pure.group', 'basket'),
        ('pure.group', 'registry'),
        ('pure.group', 'revision_metadata'),
        ('pure.group-kvstore', 'kvstore'),
        ('pure.group-kvstore', 'things'),
        ('pure.group-kvstore', 'version'),
        ('pure.group-kvstore', 'vkvstore')
    ]
