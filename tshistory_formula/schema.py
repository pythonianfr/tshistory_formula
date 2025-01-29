from pathlib import Path

from sqlhelp import sqlfile
from tshistory.schema import kvapi, tsschema

from tshistory_formula import __version__


SCHEMA = Path(__file__).parent / 'schema.sql'


class formula_schema(tsschema):

    def create(self, engine, **kw):
        super().create(engine, **kw)

        with engine.begin() as cn:
            cn.execute(sqlfile(SCHEMA, ns=self.namespace))

        tsschema(f'{self.namespace}-formula-patch').create(engine)

        kvstore = kvapi.kvstore(str(engine.url), namespace=f'{self.namespace}-kvstore')
        kvstore.set('tshistory-formula-version', __version__)
