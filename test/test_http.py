from datetime import datetime as dt
import json

import pandas as pd
import pytest
import webtest

from tshistory import util
from tshistory.testutil import (
    assert_df,
    gengroup,
    genserie,
    utcdt
)


def test_series_formula(client):
    series = genserie(utcdt(2020, 1, 1), 'D', 3)
    res = client.patch('/series/state', params={
        'name': 'test-formula',
        'series': util.tojson(series),
        'author': 'Babar',
        'insertion_date': utcdt(2020, 1, 1, 10),
        'tzaware': util.tzaware_serie(series)
    })

    assert res.status_code == 201

    res = client.patch('/series/formula', params={
        'name': 'new-formula',
        'text': '(+ 3'
    })
    assert res.status_code == 400
    assert res.json['message'] == "SyntaxError('unexpected EOF in list')"

    res = client.patch('/series/formula', params={
        'name': 'new-formula',
        'text': '(+ 3 (series "lol"))'
    })
    assert res.status_code == 409
    assert res.json['message'] == (
        "ValueError('Formula `new-formula` refers to unknown series `lol`')"
    )

    res = client.patch('/series/formula', params={
        'name': 'new-formula',
        'text': '(resample "foo")'
    })
    assert res.status_code == 409
    assert res.json['message'] == (
        'TypeError("\'foo\' not of <class \'pandas.core.series.Series\'>")'
    )

    res = client.patch('/series/formula', params={
        'name': 'new-formula',
        'text': '(+ 3 (series "lol"))',
        'reject_unknown': False
    })
    assert res.status_code == 201

    # update
    res = client.patch('/series/formula', params={
        'name': 'new-formula',
        'text': '(+ 3 (series "test-formula"))'
    })
    assert res.status_code == 200

    res = client.get('/series/state?name=new-formula')
    series = util.fromjson(res.body, 'test', True)
    assert_df("""
2020-01-01 00:00:00+00:00    3.0
2020-01-02 00:00:00+00:00    4.0
2020-01-03 00:00:00+00:00    5.0
""", series)

    res = client.get('/series/formula?name=new-formula')
    assert res.json == '(+ 3 (series "test-formula"))'

    # expansion

    res = client.patch('/series/formula', params={
        'name': '2-levels-formula',
        'text': '(+ 5 (series "new-formula"))'
    })
    res = client.get('/series/formula?name=2-levels-formula&expanded=1')
    assert res.json == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (+ 5 (+ 3 (series "test-formula")))'
        ')'
    )

    res = client.get('/series/insertion_dates', params={
        'name': 'new-formula'
    })
    idates = [
        pd.Timestamp(t, tz='UTC')
        for t in res.json['insertion_dates']
    ]
    assert idates == [
        pd.Timestamp('2020-01-01 10:00:00', tz='UTC')
    ]

    res = client.get('/series/formula_components?name=new-formula')
    assert res.json == {'new-formula': ['test-formula']}


def test_group_formula(client):
    df = gengroup(
        n_scenarios=3,
        from_date=dt(2021, 1, 1),
        length=5,
        freq='D',
        seed=2.
    )
    df.columns = ['a', 'b', 'c']

    bgroup = util.pack_group(df)
    res = client.patch('/group/state', {
        'name': 'test_group',
        'author': 'Babar',
        'format': 'tshpack',
        'replace': json.dumps(True),
        'bgroup': webtest.Upload('bgroup', bgroup)
    })
    assert res.status_code == 201

    res = client.put('/group/formula', {
        'name': 'group_formula',
        'author': 'Babar',
        'text': '(group-add (group "test_group") (group "test_group"))'
    })

    assert res.status_code == 201

    res = client.get('/group/formula', {
        'name': 'group_formula'
    })
    assert res.json == '(group-add (group "test_group") (group "test_group"))'

    res = client.get('/group/state', {'name': 'group_formula'})
    df2 = util.unpack_group(res.body)

    assert df2.equals(df * 2)


def test_bound_formula(client):
    ts = genserie(pd.Timestamp('2021-1-1'), 'H', 3)
    res = client.patch('/series/state', {
        'name': 'a-series',
        'series': util.tojson(ts),
        'author': 'Babar',
        'insertion_date': utcdt(2021, 1, 1, 10),
        'tzaware': util.tzaware_serie(ts)
    })
    res = client.patch('/series/state', {
        'name': 'another-series',
        'series': util.tojson(ts),
        'author': 'Babar',
        'insertion_date': utcdt(2021, 1, 1, 10),
        'tzaware': util.tzaware_serie(ts)
    })

    assert res.status_code == 201

    # prepare a formula
    res = client.patch('/series/formula', {
        'name': 'hijack-me',
        'text': '(add (series "a-series") (series "another-series"))'
    })
    assert res.status_code == 201

    # prepare a group
    df = gengroup(
        n_scenarios=3,
        from_date=dt(2021, 1, 1),
        length=5,
        freq='D',
        seed=2.
    )
    df.columns = ['a', 'b', 'c']

    bgroup = util.pack_group(df)
    res = client.patch('/group/state', {
        'name': 'a-group',
        'author': 'Babar',
        'format': 'tshpack',
        'replace': json.dumps(True),
        'bgroup': webtest.Upload('bgroup', bgroup)
    })
    assert res.status_code == 201

    bindings = pd.DataFrame(
        [
            ['a-series', 'a-group', 'topic'],
        ],
        columns=('series', 'group', 'family')
    )

    res = client.put('/group/boundformula', {
        'name': 'bfgroup',
        'formulaname': 'hijack-me',
        'bindings': bindings.to_json(orient='records')
    })
    assert res.status_code == 200

    res = client.get('/group/boundformula', {'name': 'bfgroup'})
    assert res.json == [
        'hijack-me', [{
            'group': 'a-group',
            'family': 'topic',
            'series': 'a-series'
        }]
    ]

    res = client.get('/group/state', {
        'name': 'bfgroup'
    })
    df2 = util.unpack_group(res.body)

    assert_df("""
              a    b    c
2021-01-01  2.0  3.0  4.0
""", df2)


def test_formula(tsx, engine, tsh):
    tsh.update(
        engine,
        pd.Series(
            [1, 2, 3],
            index=pd.date_range(
                pd.Timestamp('2020-1-1', tz='UTC'),
                freq='D',
                periods=3
            )
        ),
        'in-a-formula',
        'Babar'
    )

    with pytest.raises(SyntaxError):
        tsx.register_formula(
            'new-formula',
            '(+ 3'
        )

    with pytest.raises(ValueError):
        tsx.register_formula(
            'new-formula',
            '(+ 3 (series "lol"))'
        )

    tsx.register_formula(
        'new-formula',
        '(+ 3 (series "lol"))',
        reject_unknown=False
    )

    tsx.register_formula(
        'new-formula',
        '(+ 3 (series "in-a-formula"))',
    )

    series = tsx.get('new-formula')
    assert_df("""
2020-01-01 00:00:00+00:00    4.0
2020-01-02 00:00:00+00:00    5.0
2020-01-03 00:00:00+00:00    6.0
""", series)

    assert tsx.formula('new-formula') == '(+ 3 (series "in-a-formula"))'
    assert tsx.formula('lol') is None

    tsx.register_formula(
        '2-levels',
        '(+ 5 (series "new-formula"))'
    )
    assert tsx.formula('2-levels', display=True, expanded=True) == (
        '(+ 5 (+ 3 (series "in-a-formula")))'
    )
    assert tsx.formula('2-levels', expanded=True) == (
        '(let revision_date nil from_value_date nil to_value_date nil'
        ' (+ 5 (+ 3 (series "in-a-formula")))'
        ')'
    )

    assert tsx.formula_components('2-levels', True) == (
        {'2-levels': [{'new-formula': ['in-a-formula']}]}
    )


def test_group_formula(tsx):
    df = gengroup(
        n_scenarios=3,
        from_date=dt(2021, 1, 1),
        length=5,
        freq='D',
        seed=2.
    )
    df.columns = ['a', 'b', 'c']

    tsx.group_replace('for_a_group_formula', df, 'Babar')

    tsx.register_group_formula(
        'test_group_formula',
        '(group "for_a_group_formula")'
    )
    assert tsx.group_exists('test_group_formula')

    df2 = tsx.group_get('test_group_formula')
    assert df2.equals(df)

    assert tsx.group_formula('test_group_formula') == '(group "for_a_group_formula")'
    tsx.update_group_metadata('test_group_formula', {'foo': 'bar'})
    assert tsx.group_metadata('test_group_formula') == {'foo': 'bar'}

    tsx.group_delete('test_group_formula')
    assert not tsx.group_exists('test_group_formula')


def test_bound_formula(tsx):
    temp = pd.Series(
        [12, 13, 14],
        index=pd.date_range(utcdt(2021, 1, 1), freq='D', periods=3)
    )
    wind = pd.Series(
        [.1, .1, .1],
        index=pd.date_range(utcdt(2021, 1, 1), freq='D', periods=3)
    )

    tsx.update('base-temp', temp, 'Babar')
    tsx.update('base-wind', wind, 'Celeste')

    tsx.register_formula(
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
    tsx.group_replace(
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
    tsx.group_replace(
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

    tsx.register_formula_bindings(
        'hijacking',
        'hijacked',
        binding
    )

    b = tsx.bindings_for('hijacking')
    for attr in ('series', 'group', 'family'):
        assert b[1][attr].equals(binding[attr])

    ts = tsx.get('hijacked')
    assert_df("""
2021-01-01 00:00:00+00:00    12.1
2021-01-02 00:00:00+00:00    13.1
2021-01-03 00:00:00+00:00    14.1
""", ts)

    df = tsx.group_get('hijacking')
    assert_df("""
                             0    1
2021-01-01 00:00:00+00:00  1.0  3.0
2021-01-02 00:00:00+00:00  3.0  5.0
2021-01-03 00:00:00+00:00  5.0  7.0
""", df)

    assert tsx.group_exists('hijacking')
    assert tsx.group_type('hijacking') == 'bound'

    cat = list(tsx.group_catalog().values())[0]
    assert ('hijacking', 'bound') in cat

    assert tsx.group_metadata('hijacking') == {}
    tsx.update_group_metadata('hijacking', {'foo': 'bar'})
    assert tsx.group_metadata('hijacking') == {'foo': 'bar'}
    assert tsx.group_metadata('hijacking', all=True) == {
        'foo': 'bar',
        'index_dtype': '|M8[ns]',
        'index_type': 'datetime64[ns, UTC]',
        'tzaware': True,
        'value_dtype': '<f8',
        'value_type': 'float64'
    }

    tsx.group_delete('hijacking')
    assert not tsx.group_exists('hijacking')

    assert tsx.group_metadata('hijacking') is None
