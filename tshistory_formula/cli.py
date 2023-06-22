import hashlib

import click
from sqlalchemy import create_engine
from psyl.lisp import (
    parse,
    serialize
)
from tshistory.util import find_dburi

from tshistory_formula.schema import formula_schema
from tshistory_formula.tsio import timeseries
from tshistory_formula.helper import rename_operator
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


@click.command(name='migrate-to-content-hash')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_content_hash(db_uri, namespace='tsh'):
    from psyl import lisp
    engine = create_engine(find_dburi(db_uri))
    tsh = timeseries(namespace)

    chs = []
    series = engine.execute(
        f'select name, text from "{namespace}".formula'
    ).fetchall()
    print(f'Preparing {len(series)}.')

    for idx, (name, text) in enumerate(series):
        print(idx, name)
        ch = hashlib.sha1(
            lisp.serialize(
                tsh._expanded_formula(engine, text)
            ).encode()
        ).hexdigest()
        chs.append(
            {'name': name, 'contenthash': ch}
        )

    sql = (
        f'alter table "{namespace}".formula '
        'add column if not exists contenthash text not null default \'\';'
    )

    with engine.begin() as cn:
        cn.execute(sql)
        cn.execute(
            f'update "{namespace}".formula '
            f'set contenthash = %(contenthash)s '
            f'where name = %(name)s',
            chs
        )

        cn.execute(
            f'alter table "{namespace}".formula '
            f'alter column contenthash drop default;'
        )


@click.command(name='rename-operators')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def rename_operators(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))

    def rename_series(series):
        rewritten = []
        print(f'Transforming {len(series)} series.')
        for idx, (name, text) in enumerate(series):
            print(idx, name, text)
            tree0 = parse(text)
            tree1 = rename_operator(tree0, 'min', 'row-min')
            tree2 = rename_operator(tree1, 'max', 'row-max')
            tree3 = rename_operator(tree2, 'timedelta', 'shifted')
            tree4 = rename_operator(tree3, 'shift', 'time-shifted')
            rewritten.append(
                {'name': name, 'text': serialize(tree4)}
            )
        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".formula '
                f'set text = %(text)s '
                f'where name = %(name)s',
                rewritten
            )

    series = engine.execute(
        f'select name, text from "{namespace}".formula'
    ).fetchall()

    if series:
        rename_series(series)

    def rename_groups(series):
        rewritten = []
        print(f'Transforming {len(groups)} groups.')
        for idx, (name, text) in enumerate(series):
            print(idx, name, text)
            tree0 = parse(text)
            tree1 = rename_operator(tree0, 'min', 'row-min')
            tree2 = rename_operator(tree1, 'max', 'row-max')
            tree3 = rename_operator(tree2, 'timedelta', 'shifted')
            tree4 = rename_operator(tree3, 'shift', 'time-shifted')
            rewritten.append(
                {'name': name, 'text': serialize(tree4)}
            )
        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".group_formula '
                f'set text = %(text)s '
                f'where name = %(name)s',
                rewritten
            )

    groups = engine.execute(
        f'select name, text from "{namespace}".group_formula'
    ).fetchall()

    if groups:
        rename_groups(groups)


@click.command(name='migrate-to-dependants')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_dependants(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))

    sql = """
create table if not exists "{ns}".dependant (
  sid int not null references "{ns}".formula(id) on delete cascade,
  needs int not null references "{ns}".formula(id) on delete cascade,
  unique(sid, needs)
);

create index if not exists "ix_{ns}_dependant_sid" on "{ns}".dependant (sid);
create index if not exists "ix_{ns}_dependant_needs" on "{ns}".dependant (needs);
""".format(ns=namespace)

    with engine.begin() as cn:
        cn.execute(sql)

    with engine.begin() as cn:
        # purge
        cn.execute(f'delete from "{namespace}".dependant')

    series = engine.execute(
        f'select name, text from "{namespace}".formula'
    ).fetchall()
    tsh = timeseries(namespace)

    for name, text in series:
        tsh.register_dependants(engine, name, parse(text))


@click.command(name='shell')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def shell(db_uri, namespace='tsh'):
    from tshistory.api import timeseries as tsapi
    tsa = tsapi(find_dburi(db_uri), namespace, timeseries)  # noqa: F841
    import pdb; pdb.set_trace()
