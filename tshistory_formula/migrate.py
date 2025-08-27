import json

from psyl.lisp import (
    parse,
    serialize
)

from tshistory.migrate import (
    do_cleanup_kvstore,
    do_enforce_series_metadata_integrity,
    Migrator as _Migrator,
    version
)

from tshistory_formula import __version__
from tshistory_formula.helper import (
    fix_holidays,
    FREQ_OPS,
    migrate_freq,
    TIMEZONE_OPS,
    migrate_timezone,
    migrate_fix_day_freq,
    rebuild_dependencies,
    rename_operator,
    rewrite_groupadd_formula,
    rewrite_sub_formula,
    rewrite_trig_formula
)


class Migrator(_Migrator):
    _order = 1
    _package_version = __version__
    _package = 'tshistory-formula'

    def initial_migration(self):
        print('initial migration')
        migrate_formula_schema(self.engine, self.namespace, self.interactive)
        migrate_to_formula_patch(self.engine, self.namespace, self.interactive)
        migrate_trig_formulas(self.engine, self.namespace, self.interactive)
        migrate_sub_formulas(self.engine, self.namespace, self.interactive)
        migrate_group_formula_schema(self.engine, self.namespace, self.interactive)


@version('tshistory-formula', '0.19.0')
def migrate_0190(engine, namespace, interactive):
    _migrate_form_history_table(engine, namespace, interactive)
    do_cleanup_kvstore(engine, f'{namespace}-formula-patch', interactive)
    _migrate_fix_formula_indexes(engine, namespace, interactive)
    _migrate_formula_patch_metadata_integrity(engine, namespace, interactive)
    _migrate_rebuild_dependencies(engine, namespace, interactive)


def _migrate_form_history_table(engine, namespace, interactive):
    """Create form_history table for formula version tracking"""
    ns = namespace
    with engine.begin() as cn:
        cn.execute(f"""
create table if not exists "{ns}".form_history (
  sid int not null references "{ns}".registry(id) on delete cascade,
  archivedate timestamptz not null default now(),
  userid text,
  formula text not null
);

create index if not exists "ix_{ns}_form_history_sid" on "{ns}".form_history(sid);
""", _binary=False)


def _migrate_fix_formula_indexes(engine, namespace, interactive):
    """Fix indexes in both namespaces used by formula"""
    from pathlib import Path
    from tshistory.migrate import (
        create_revision_metadata_for_ns,
        do_fix_indexes
    )
    from tshistory.sqlparser import (
        parse_indexes,
        TSHISTORY_PATH,
        TSHISTORY_SQLFILES
    )

    create_revision_metadata_for_ns(engine, f'{namespace}-formula-patch')

    formula_path = Path(__file__).parent
    sqlfiles = TSHISTORY_SQLFILES + (formula_path / 'schema.sql',)
    main_indexes = parse_indexes(sqlfiles, namespace)
    do_fix_indexes(engine, namespace, interactive, main_indexes)

    patch_indexes = parse_indexes(
        (TSHISTORY_PATH / 'registry.sql',),
        f'{namespace}-formula-patch'
    )
    do_fix_indexes(engine, f'{namespace}-formula-patch', interactive, patch_indexes)


def _migrate_formula_patch_metadata_integrity(engine, namespace, interactive):
    # apply metadata integrity migration to formula-patch namespace
    do_enforce_series_metadata_integrity(engine, f'{namespace}-formula-patch', interactive)


def _migrate_rebuild_dependencies(engine, namespace, interactive):
    print('rebuilding formula dependencies')
    from tshistory_formula.tsio import timeseries
    tsh = timeseries(namespace)

    with engine.begin() as cn:
        rebuild_dependencies(cn, tsh)


@version('tshistory-formula', '0.18.0')
def migrate_0180(engine, namespace, interactive):
    _migrate_formula_history(engine, namespace, interactive)
    _migrate_bound_groups(engine, namespace, interactive)
    _migrate_freq_and_timezone(engine, namespace, interactive)
    _migrate_fix_bad_day_freq(engine, namespace, interactive)


def _migrate_formula_history(engine, namespace, interactive):
    ns = namespace
    sql = (
        f'create table if not exists "{ns}".form_history ('
        f'  sid int not null references "{ns}".registry(id) on delete cascade,'
        f'  archivedate timestamptz not null unique default now(),'
        f'  formula text not null'
        f');'
    )
    with engine.begin() as cn:
        cn.execute(sql)
        cn.execute(
            f'create index if not exists "ix_{ns}_form_history_sid" on "{ns}".form_history(sid)'
        )


def _migrate_bound_groups(engine, namespace, interactive):
    import json
    import pandas as pd
    from tshistory_formula.tsio import timeseries
    from tshistory_formula import helper

    tsh  = timeseries(namespace)
    ns = namespace
    newtables = f"""
create table if not exists "{ns}".group_binding (
  id serial primary key,
  groupid int not null references "{ns}".group_registry(id) on delete cascade,
  seriesid int not null references "{ns}".registry(id) on delete cascade
);

create index if not exists "ix_{ns}_group_binding_groupid" on "{ns}".group_binding (groupid);
create index if not exists "ix_{ns}_group_binding_seriesid" on "{ns}".group_binding (seriesid);

create table if not exists "{ns}".group_series_map (
  family text not null,
  parent int not null references "{ns}".group_binding(id) on delete cascade,
  groupid int not null references "{ns}".group_registry(id) on delete cascade,
  seriesid int not null references "{ns}".registry(id) on delete cascade,
  unique(family, parent, groupid, seriesid)
);

create index if not exists "ix_{ns}_group_series_map_parent" on "{ns}".group_series_map (parent);
create index if not exists "ix_{ns}_group_series_map_family" on "{ns}".group_series_map (family);
create index if not exists "ix_{ns}_group_series_map_groupid" on "{ns}".group_series_map (groupid);
create index if not exists "ix_{ns}_group_series_map_seriesid" on "{ns}".group_series_map (seriesid);
"""
    with engine.begin() as cn:
        cn.execute(newtables, _binary=False)

    with engine.begin() as cn:
        sql = (
            f'select name from "{ns}".group_registry '
            'where internal_metadata -> \'bindings\' is not null'
        )
        for name, in cn.execute(sql).fetchall():
            res = cn.execute(
                f'select id, '
                f'       internal_metadata->\'boundseries\', '
                f'       internal_metadata->\'bindings\' '
                f'from "{namespace}".group_registry '
                'where name = %(gname)s',
                gname=name
            )
            groupid, formulaname, binding = res.fetchone()
            binding = pd.DataFrame(json.loads(binding))

            # recreate the binding
            helper.new_bound_formula(cn, namespace, groupid, formulaname, binding)

            # cleanup the internal metadata
            imeta = tsh.group_internal_metadata(cn, name)
            imeta.pop('boundseries')
            imeta.pop('bindings')
            imeta['bound'] = True
            cn.execute(
                f'update "{namespace}".group_registry '
                'set internal_metadata = %(imeta)s '
                f'where name = %(name)s',
                imeta=json.dumps(imeta),
                name=name
            )


def _migrate_freq_and_timezone(engine, namespace, interactive):
    with engine.begin() as cn:
        formulas = cn.execute(
            f'select name, internal_metadata '
            f'from "{namespace}".registry '
            'where internal_metadata->\'formula\' is not null'
        ).fetchall()
    op_names = list(FREQ_OPS) + list(TIMEZONE_OPS)

    for name, imeta in formulas:
        text = imeta['formula']
        do_migrate = False
        for op_name in op_names:
            if op_name in text:
                do_migrate = True
        if not do_migrate:
            continue

        tree = migrate_timezone(migrate_freq(parse(text)))
        imeta['formula'] = serialize(tree)
        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".registry '
                'set internal_metadata = %(metadata)s '
                'where name = %(name)s',
                metadata=json.dumps(imeta),
                name=name
            )


def _migrate_fix_bad_day_freq(engine, namespace, interactive):
    with engine.begin() as cn:
        formulas = cn.execute(
            f'select name, internal_metadata '
            f'from "{namespace}".registry '
            'where internal_metadata->\'formula\' is not null'
        ).fetchall()

    for name, imeta in formulas:
        text = imeta['formula']
        tree = migrate_fix_day_freq(parse(text))
        imeta['formula'] = serialize(tree)
        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".registry '
                'set internal_metadata = %(metadata)s '
                'where name = %(name)s',
                metadata=json.dumps(imeta),
                name=name
            )


@version('tshistory-formula', '0.17.0')
def do_migrate_intervals(engine, namespace, interactive):
    from tshistory.migrate import migrate_intervals

    migrate_intervals(engine, f'{namespace}-formula-patch', interactive)

    with engine.begin() as cn:
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch".basket'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch".groupmap'
        )
        cn.execute(
            f'drop table if exists "{namespace}-formula-patch".group_registry'
        )
        cn.execute(
            f'drop table if exists "{namespace}-group".basket'
        )


@version('tshistory-formula', '0.16.1')
def migrate_holidays_operator(engine, namespace, interactive):
    formulas = engine.execute(
        f'select name, internal_metadata '
        f'from "{namespace}".registry '
        'where internal_metadata->\'formula\' is not null'
    ).fetchall()

    for name, imeta in formulas:
        text = imeta['formula']
        if '(holidays' in text:
            print(f' fixing `holidays` for {name}')
        else:
            continue

        tree = fix_holidays(parse(text))
        imeta['formula'] = serialize(tree)
        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".registry '
                'set internal_metadata = %(metadata)s '
                'where name = %(name)s',
                metadata=json.dumps(imeta),
                name=name
            )


@version('tshistory-formula', '0.16.0')
def migrate_revision_table(engine, namespace, interactive):
    from tshistory.migrate import migrate_add_diffstart_diffend

    migrate_add_diffstart_diffend(engine, f'{namespace}-formula-patch', interactive)


@version('tshistory-formula', '0.16.0')
def rename_today_operator(engine, namespace, interactive):
    formulas = engine.execute(
        f'select name, internal_metadata '
        f'from "{namespace}".registry '
        'where internal_metadata->\'formula\' is not null'
    ).fetchall()

    for name, imeta in formulas:
        text = imeta['formula']
        if '(today)' in text:
            print(f' renaming `today` -> `now` for {name}')
        else:
            continue
        tree = rename_operator(
            parse(text),
            'today',
            'now'
        )
        imeta['formula'] = serialize(tree)
        with engine.begin() as cn:
            cn.execute(
                f'update "{namespace}".registry '
                'set internal_metadata = %(metadata)s '
                'where name = %(name)s',
                metadata=json.dumps(imeta),
                name=name
            )


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

            if imeta is None:
                print(f'Skipping corrupt {name} series. You should remove it.')
                continue

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
        for fid, name, formula, imeta in cn.execute(
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


def migrate_groupadd_formulas(engine, namespace, interactive):
    print('migrate group-add formulas')
    from tshistory_formula.tsio import timeseries
    tsh = timeseries(namespace)  # noqa: F841

    def reorganise_groupadd_groups(groups):
        rewritten = []
        print(f'Transforming {len(groups)} groups.')
        for idx, (name, internal_metadata) in enumerate(groups):
            tree0 = parse(internal_metadata['formula'])
            tree1 = rewrite_groupadd_formula(tree0)
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

    groups = engine.execute(
        f'select name, internal_metadata from "{namespace}".group_registry '
        'where internal_metadata->\'formula\' is not null'
    ).fetchall()

    if groups:
        reorganise_groupadd_groups(groups)
