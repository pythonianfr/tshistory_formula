import json

import pandas as pd
from psyl.lisp import (
    parse,
    serialize
)

from tshistory.migrate import Migrator as _Migrator

from tshistory_formula import __version__
from tshistory_formula.helper import (
    rewrite_sub_formula,
    rewrite_trig_formula
)


class Migrator(_Migrator):
    _order = 1
    _known_version = __version__
    _package = 'tshistory-formula'

    def initial_migration(self):
        print('initial migration')
        migrate_formula_schema(self.engine, self.namespace, self.interactive)
        migrate_to_formula_patch(self.engine, self.namespace, self.interactive)
        migrate_trig_formulas(self.engine, self.namespace, self.interactive)
        migrate_sub_formulas(self.engine, self.namespace, self.interactive)
        fix_formula_groups_metadata(self.engine, self.namespace, self.interactive)
        migrate_group_formula_schema(self.engine, self.namespace, self.interactive)


def migrate_formula_schema(engine, namespace, interactive):
    print('migrate formula schema')
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
        metakeys = tshclass.metakeys
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


def migrate_group_formula_schema(engine, namespace, interactive):
    print('migrate group formula schema')
    ns = namespace
    from tshistory.tsio import timeseries as tshclass

    with engine.begin() as cn:
        unmigrated = cn.execute(
            f"select exists (select 1 "
            f"  from information_schema.columns "
            f"  where table_schema='{ns}' and "
            f"        table_name='group_formula'"
            f")"
        ).scalar()

        if not unmigrated:
            print('Already migrated, bailing out.')
            return

        print('migrating group data.')
        allmetas = {}
        metakeys = tshclass.metakeys

        # collect from group_formula and reinsert
        for fid, name, formula, imeta, contenthash in cn.execute(
                'select id, name, text, metadata '
                f'from "{ns}".group_formula '
        ).fetchall():
            umeta = {}
            for k in list(imeta):
                if k not in metakeys:
                    umeta[k] = imeta.pop(k)
            imeta['formula'] = formula
            allmetas[name] = (fid, imeta, umeta)

        # store them
        for name, (fid, imeta, umeta) in allmetas.items():
            cn.execute(
                f'insert into "{ns}".group_registry '
                '(name, internal_metadata, metadata) '
                'values(%(name)s, %(imeta)s, %(umeta)s) '
                'returning id',
                name=name,
                imeta=json.dumps(imeta),
                umeta=json.dumps(umeta)
            ).scalar()

        # collect from group_binding and reinsert
        allmetas = {}
        for fid, name, seriesname, bindings, imeta in cn.execute(
                'select id, groupname, seriesname, binding, metadata '
                f'from "{ns}".group_binding '
        ).fetchall():
            umeta = {}
            for k in list(imeta):
                if k not in metakeys:
                    umeta[k] = imeta.pop(k)
            imeta['boundseries'] = seriesname
            imeta['bindings'] = json.dumps(bindings)
            allmetas[name] = (fid, imeta, umeta)

        # store them
        for name, (fid, imeta, umeta) in allmetas.items():
            cn.execute(
                f'insert into "{ns}".group_registry '
                '(name, internal_metadata, metadata) '
                'values(%(name)s, %(imeta)s, %(umeta)s) '
                'returning id',
                name=name,
                imeta=json.dumps(imeta),
                umeta=json.dumps(umeta)
            ).scalar()

        # goodby formula/binding tables !
        cn.execute(f'drop table "{ns}".group_formula')
        cn.execute(f'drop table "{ns}".group_binding')


def migrate_to_formula_patch(engine, namespace, interactive):
    print('migrate to formula patch')
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


def migrate_trig_formulas(engine, namespace, interactive):
    print('migrate trig formulas')
    from tshistory_formula.tsio import timeseries
    tsh = timeseries(namespace)  # noqa: F841

    def reorganise_trig_series(series):
        rewritten = []
        print(f'Transforming {len(series)} series.')
        for idx, (name, internal_metadata) in enumerate(series):
            print('name', name)
            print('internal_metadata', internal_metadata)
            tree0 = parse(internal_metadata['formula'])
            tree1 = rewrite_trig_formula(tree0)
            internal_metadata['formula'] = serialize(tree1)
            rewritten.append(
                {'name': name, 'internal_metadata': json.dumps(internal_metadata)}
            )

        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".registry '
                f'set internal_metadata = %(internal_metadata)s '
                f'where name = %(name)s',
                rewritten
            )

    series = engine.execute(
        f'select name, internal_metadata from "{namespace}".registry '
        'where internal_metadata->\'formula\' is not null'
    ).fetchall()

    if series:
        reorganise_trig_series(series)


def migrate_sub_formulas(engine, namespace, interactive):
    print('migrate sub formulas')
    from tshistory_formula.tsio import timeseries
    tsh = timeseries(namespace)  # noqa: F841

    def reorganise_sub_series(series):
        rewritten = []
        print(f'Transforming {len(series)} series.')
        for idx, (name, internal_metadata) in enumerate(series):
            print('name', name)
            print('internal_metadata', internal_metadata)
            tree0 = parse(internal_metadata['formula'])
            tree1 = rewrite_sub_formula(tree0)
            internal_metadata['formula'] = serialize(tree1)
            rewritten.append(
                {'name': name, 'internal_metadata': json.dumps(internal_metadata)}
            )

        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".registry '
                f'set internal_metadata = %(internal_metadata)s '
                f'where name = %(name)s',
                rewritten
            )

    series = engine.execute(
        f'select name, internal_metadata from "{namespace}".registry '
        'where internal_metadata->\'formula\' is not null'
    ).fetchall()

    if series:
        reorganise_sub_series(series)


def fix_formula_groups_metadata(engine, namespace, interactive):
    from tshistory_formula.tsio import timeseries
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
