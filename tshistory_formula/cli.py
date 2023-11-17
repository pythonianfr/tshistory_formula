import click
from sqlalchemy import create_engine
from psyl.lisp import parse
from tshistory.util import find_dburi

from tshistory_formula.schema import formula_schema
from tshistory_formula.tsio import timeseries
from tshistory_formula.types import typecheck
from tshistory_formula.interpreter import Interpreter


@click.command(name='typecheck-formula')
@click.argument('db-uri')
@click.option('--pdbshell', is_flag=True, default=False)
@click.option('--namespace', default='tsh')
def typecheck_formula(db_uri, pdbshell=False, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))
    tsh = timeseries(namespace)

    i = Interpreter(engine, tsh, {})
    for name, kind in tsh.list_series(engine).items():
        if kind != 'formula':
            continue

        formula = tsh.formula(engine, name)
        parsed = parse(formula)
        print(name, f'`{parsed[0]}`')
        typecheck(parsed, env=i.env)

    if pdbshell:
        import ipdb; ipdb.set_trace()


@click.command(name='test-formula')
@click.argument('db-uri')
@click.argument('formula')
@click.option('--pdbshell', is_flag=True, default=False)
@click.option('--namespace', default='tsh')
def test_formula(db_uri, formula, pdbshell=False, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))
    tsh = timeseries(namespace)

    ts = tsh.eval_formula(engine, formula)
    print(ts)
    if pdbshell:
        import ipdb; ipdb.set_trace()


@click.command(name='formula-init-db')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def init_db(db_uri, namespace):
    "initialize the formula part of a timeseries history schema"
    engine = create_engine(find_dburi(db_uri))
    formula_schema(namespace).create(engine)


# migration


@click.command(name='fix-formula-groups-metadata')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def fix_formula_groups_metadata_(db_uri, namespace='tsh'):
    from tshistory_formula.migrate import fix_formula_groups_metadata

    engine = create_engine(find_dburi(db_uri))
    fix_formula_groups_metadata(engine, namespace, True)


# migration
@click.command(name='migrate-to-formula-groups')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_groups(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))

    ns = namespace
    sql = f"""
    create table if not exists "{ns}".group_formula (
      id serial primary key,
      name text unique not null,
      text text not null,
      metadata jsonb
    );

    create table if not exists "{ns}".group_binding (
      id serial primary key,
      groupname text unique not null,
      seriesname text not null,
      binding jsonb not null,
      metadata jsonb
    );
    """

    with engine.begin() as cn:
        cn.execute(sql)


