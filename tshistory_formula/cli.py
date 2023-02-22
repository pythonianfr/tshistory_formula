import json
from pprint import pprint
import hashlib

import click
import pandas as pd
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
@click.command(name='migrate-formula-schema')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_formula_schema(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))
    ns = namespace
    from tshistory.tsio import timeseries as tshclass

    with engine.begin() as cn:
        unmigrated = cn.execute(
            f"select exists (select 1 "
            f"  from information_schema.columns "
            f"  where table_schema='{ns}' and "
            f"        table_name='formula'"
            f")"
        ).scalar()

        if not unmigrated:
            print('Already migrated, bailing out.')
            return

        print('migrating data.')
        allmetas = {}
        metakeys = tshclass.metakeys | {'supervision_status'}
        for fid, name, formula, imeta, contenthash in cn.execute(
                'select id, name, text, metadata, contenthash '
                f'from "{ns}".formula '
        ).fetchall():
            umeta = {}
            for k in list(imeta):
                if k not in metakeys:
                    umeta[k] = imeta.pop(k)
            imeta['formula'] = formula
            imeta['contenthash'] = contenthash
            allmetas[name] = (fid, imeta, umeta)

        idmap = {}
        # store them
        for name, (fid, imeta, umeta) in allmetas.items():
            sid = cn.execute(
                f'insert into "{ns}".registry '
                '(name, internal_metadata, metadata) '
                'values(%(name)s, %(imeta)s, %(umeta)s) '
                'returning id',
                name=name,
                imeta=json.dumps(imeta),
                umeta=json.dumps(umeta)
            ).scalar()
            idmap[fid] = sid

        # collect dependency tree
        deps = {}
        for fid, needs in cn.execute(
                f'select sid, needs from "{ns}".dependant'
        ).fetchall():
            deps[fid] = needs

        # let's be brutal: drop and recreate
        cn.execute(
            f'drop table "{ns}".dependant'
        )
        cn.execute(f"""
        create table "{ns}".dependent (
          sid int not null references "{ns}".registry(id) on delete cascade,
          needs int not null references "{ns}".registry(id) on delete cascade,
          unique(sid, needs)
        );

        create index "ix_{ns}_dependent_sid" on "{ns}".dependent (sid);
        create index "ix_{ns}_dependent_needs" on "{ns}".dependent (needs);
        """)

        # rebuild dependency tree
        for fid, needs in deps.items():
            cn.execute(
                f'insert into "{ns}".dependent '
                '(sid, needs) '
                'values(%(sid)s, %(needs)s)',
                sid=idmap[fid],
                needs=idmap[needs]
            )

        # last: migrate cache_policy_series
        # at this point everything is ready
        # and we have an id map from the old formula table
        # to the registry table
        # esp: series_id int unique not null references "{ns}".formula -> registry
        # That means we have no choice but to migrate this refinery table
        # there.
        # Let's start collecting its content, drop it, spawn the new schema
        # and re-populate it using idmap ...
        cps = cn.execute(
            f'select cache_policy_id, series_id '
            f'from "{ns}".cache_policy_series '
        ).fetchall()
        newcps = [
            (cpi, idmap[sid])
            for cpi, sid in cps
        ]
        cn.execute(f'drop table "{ns}".cache_policy_series')
        cn.execute(
            f"""
            create table "{ns}".cache_policy_series (
                cache_policy_id int not null references "{ns}".cache_policy on delete cascade,
                series_id int unique not null references "{ns}".registry on delete cascade,
                ready bool not null default true,

                unique (cache_policy_id, series_id)
            );

            create index on "{ns}".cache_policy_series (cache_policy_id);
            create index on "{ns}".cache_policy_series (series_id);
            """
        )
        for cpi, si in newcps:
            cn.execute(
                f'insert into "{ns}".cache_policy_series '
                '(cache_policy_id, series_id) '
                'values(%(cpi)s, %(si)s)',
                cpi=cpi,
                si=si
            )
        # goodby formula table !
        cn.execute(f'drop table "{ns}".formula')


@click.command(name='fix-formula-groups-metadata')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def fix_formula_groups_metadata(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))
    tsh = timeseries(namespace)

    formulas = []
    bound = []

    for name, kind in tsh.list_groups(engine).items():
        if kind == 'primary':
            continue

        if kind == 'formula':
            formulas.append(
                (name, tsh.group_formula(engine, name))
            )
            continue

        assert kind == 'bound'
        sname, bindings = engine.execute(
            f'select seriesname, binding '
            f'from "{namespace}".group_binding '
            'where groupname = %(name)s',
            name=name
        ).fetchone()

        bound.append(
            (name, sname, bindings)
        )

    print(f'collected {len(formulas)} formulas to migrate')
    print(f'collected {len(bound)} bindings to migrate')

    for name, formula in formulas:
        with engine.begin() as cn:
            tsh.group_delete(cn, name)
            tsh.register_group_formula(cn, name, formula)

    invalid = []
    for name, sname, bindings in bound:
        with engine.begin() as cn:
            tsh.group_delete(cn, name)
            try:
                tsh.register_formula_bindings(
                    cn,
                    name,
                    sname,
                    pd.DataFrame(bindings),
                    nockeck=True
                )
            except Exception as err:
                invalid.append(
                    (name, err)
                )

    if invalid:
        print('Invalid bound groups could not be fixed:')
        for name, err in invalid:
            print(f'{name}: {err}')


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
    tsh = timeseries(namespace)

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


@click.command(name='migrate-to-formula-patch')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def migrate_to_formula_patch(db_uri, namespace='tsh'):
    engine = create_engine(find_dburi(db_uri))
    ns_name = f'{namespace}-formula-patch'
    with engine.begin() as cn:
        exists = cn.execute(
            'select 1 from information_schema.schemata where schema_name = %(name)s',
            name=ns_name
        ).scalar()
        if exists:
            print(f'Schema `{ns_name}` already exists. Nothing to do.')
            return
    from tshistory.schema import tsschema
    schem = tsschema(ns_name)
    schem.create(engine)


@click.command(name='shell')
@click.argument('db-uri')
@click.option('--namespace', default='tsh')
def shell(db_uri, namespace='tsh'):
    from tshistory.api import timeseries as tsapi
    tsa = tsapi(find_dburi(db_uri), namespace, timeseries)
    import pdb; pdb.set_trace()
