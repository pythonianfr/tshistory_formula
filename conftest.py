from pathlib import Path

import pytest
from sqlhelp.pgapi import pgdb as create_engine
from sqlhelp.testutil import setup_local_pg_cluster
import webtest
from click.testing import CliRunner

from tshistory import cli as command, api
from tshistory.http.util import nosecurity
from tshistory.testutil import (
    make_tsx,
    tempconfig
)

from tshistory_formula.schema import formula_schema
from tshistory_formula.testutil import with_http_bridge
from tshistory_formula.tsio import timeseries
from tshistory_formula import http


DATADIR = Path(__file__).parent / 'test' / 'data'
DBURI = 'postgresql://localhost:5434/postgres'


@pytest.fixture(scope='session')
def engine(request):
    setup_local_pg_cluster(request, DATADIR, 5434)
    e = create_engine(DBURI)
    return e


@pytest.fixture(scope='session', params=[1, 16])
def tsh(request, engine):
    formula_schema().create(engine)
    tsh = timeseries()
    tsh.concurrency = request.param
    yield tsh


@pytest.fixture(scope='session')
def tsa(engine):
    formula_schema().create(engine)
    formula_schema('remote').create(engine)
    config = (
        f'[dburi]\n'
        f'test = {str(engine.url)}\n'
    ).encode()
    with tempconfig(config):
        yield api.timeseries(
            str(engine.url),
            namespace='tsh',
            handler=timeseries,
            sources={'remote': (str(engine.url), 'remote')}
        )


@pytest.fixture
def cli():
    def runner(*args, **kw):
        args = [str(a) for a in args]
        for k, v in kw.items():
            if isinstance(v, bool):
                if v:
                    args.append('--{}'.format(k))
            else:
                args.append('--{}'.format(k))
                args.append(str(v))
        return CliRunner().invoke(command.tsh, args)
    return runner


# support for the http extensions

def make_app(tsa):
    from flask import Flask
    from tshistory_formula.http import formula_httpapi
    app = Flask(__name__)
    api = formula_httpapi(tsa)
    app.register_blueprint(
        api.bp
    )
    return app

# Error-displaying web tester

class WebTester(webtest.TestApp):

    def _check_status(self, status, res):
        try:
            super(WebTester, self)._check_status(status, res)
        except:
            print('ERRORS', res.errors)
            # raise <- default behaviour on 4xx is silly


@pytest.fixture(scope='session')
def client(engine):
    formula_schema().create(engine)
    wsgi = nosecurity(
        make_app(
            api.timeseries(
                str(engine.url),
                handler=timeseries,
                namespace='tsh',
                sources={}
            )
        )
    )
    yield WebTester(wsgi)


def _initschema(engine):
    formula_schema('tsh').create(engine)
    formula_schema('remote').create(engine)


tsx = make_tsx(
    'http://test.me',
    _initschema,
    timeseries,
    http.formula_httpapi,
    http.formula_httpclient,
    with_http_bridge=with_http_bridge,
    sources={'remote': (DBURI, 'remote')}
)
