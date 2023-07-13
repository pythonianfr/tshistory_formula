from pathlib import Path

import pytest
from sqlalchemy import create_engine
import webtest
from pytest_sa_pg import db
from click.testing import CliRunner

from tshistory import cli as command, api
from tshistory.testutil import make_tsx
from tshistory.schema import tsschema

from tshistory_formula.schema import formula_schema
from tshistory_formula.testutil import with_http_bridge
from tshistory_formula.tsio import timeseries
from tshistory_formula import http

DATADIR = Path(__file__).parent / 'data'


@pytest.fixture(scope='session')
def engine(request):
    port = 5433
    db.setup_local_pg_cluster(request, DATADIR, port)
    uri = 'postgresql://localhost:{}/postgres'.format(port)
    e = create_engine(uri)
    return e


@pytest.fixture(scope='session', params=[1, 16])
def tsh(request, engine):
    tsschema().create(engine, reset=True)
    formula_schema().create(engine)
    tsh = timeseries()
    tsh.concurrency = request.param
    yield tsh


@pytest.fixture(scope='session')
def tsa(engine):
    tsschema('test-mapi').create(engine, reset=True)
    formula_schema('test-mapi').create(engine)
    tsschema('test-mapi-2').create(engine, reset=True)
    formula_schema('test-mapi-2').create(engine)
    return api.timeseries(
        str(engine.url),
        namespace='test-mapi',
        handler=timeseries,
        sources={'remote': (str(engine.url), 'test-mapi-2')}
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


@pytest.fixture(scope='session')
def datadir():
    return DATADIR


# support for the http extensions
DBURI = 'postgresql://localhost:5433/postgres'

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
    tsschema().create(engine, reset=True)
    formula_schema().create(engine)
    wsgi = make_app(
        api.timeseries(
            str(engine.url),
            handler=timeseries,
            namespace='tsh',
            # sources=[(DBURI, 'other')]
        )
    )
    yield WebTester(wsgi)


def _initschema(engine):
    tsschema('tsh').create(engine, reset=True)
    formula_schema('tsh').create(engine)



tsx = make_tsx(
    'http://test.me',
    _initschema,
    timeseries,
    http.formula_httpapi,
    http.formula_httpclient,
    with_http_bridge=with_http_bridge
)
