import json
from dbcache import api as dbapi

from tshistory.util import read_versions


def run_migrations(engine, namespace, interactive=False):
    print('Running migrations for tshistory_formula.')
    # determine versions
    storens = f'{namespace}-kvstore'
    stored_version, known_version = read_versions(
        str(engine.url),
        namespace,
        'tshistory-formula-version'
    )

    if stored_version is None:
        # first time
        from tshistory_formula import __version__ as known_version
        store = dbapi.kvstore(str(engine.url), namespace=storens)
        initial_migration(engine, namespace, interactive)
        store.set('tshistory-formula-version', known_version)


def initial_migration(engine, namespace, interactive):
    print('initial migration')
    migrate_formula_schema(engine, namespace, interactive)


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
