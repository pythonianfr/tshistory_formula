from pathlib import Path

from sqlhelp import sqlfile
from tshistory.schema import tsschema


SCHEMA = Path(__file__).parent / 'schema.sql'


class formula_schema(tsschema):

    def create(self, engine, **kw):
        super().create(engine, **kw)

        with engine.begin() as cn:
            cn.execute(sqlfile(SCHEMA, ns=self.namespace))

        tsschema(f'{self.namespace}-formula-patch').create(engine)
