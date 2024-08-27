from collections import defaultdict
import hashlib
import itertools
import json
import logging

import pandas as pd
from psyl.lisp import (
    parse,
    serialize
)
from tshistory.tsio import timeseries as basets
from tshistory.util import (
    compatible_date,
    empty_series,
    patch,
    ts,
    tx
)

from tshistory_formula import funcs, gfuncs  # trigger registration  # noqa: F401
from tshistory_formula import (
    api,  # trigger extension  # noqa: F401
    interpreter,
    helper,
    types
)
from tshistory_formula.registry import (
    FINDERS,
    FUNCS,
    IDATES,
    METAS,
    GFINDERS,
    GAUTO,
    GIDATES,
    GMETAS
)


L = logging.getLogger('tshistory.tsio')


class timeseries(basets):
    index = 1

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.patch = basets(
            namespace=f'{self.namespace}-formula-patch'
        )

    fast_staircase_operators = set(['+', '*', 'series', 'add', 'priority'])
    metadata_compat_excluded = ()
    concurrency = 16

    def find_series(self, cn, tree):
        tree = helper.replace_findseries(cn, self, tree)
        return self._find_series(cn, tree)

    def _find_series(self, cn, tree):
        op = tree[0]
        finder = FINDERS.get(op)
        seriestree = finder(cn, self, tree) if finder else {}
        for item in tree:
            if isinstance(item, list):
                seriestree.update(
                    self.find_series(cn, item)
                )
        return seriestree

    def find_metas(self, cn, tree):
        op = tree[0]
        metas = METAS.get(op)
        seriesmeta = metas(cn, self, tree) if metas else {}
        for item in tree:
            if isinstance(item, list):
                seriesmeta.update(
                    self.find_metas(cn, item)
                )
        return seriesmeta

    def find_callsites(self, cn, operator, tree):
        op = tree[0]
        sites = []
        if op == operator:
            sites.append(tree)
        for item in tree:
            if isinstance(item, list):
                sites.extend(
                    self.find_callsites(cn, operator, item)
                )
        return sites

    def find_operators(self, cn, tree):
        ops = {
            tree[0]: FUNCS.get(tree[0])
        }
        for item in tree:
            if isinstance(item, list):
                newops = self.find_operators(cn, item)
                ops.update(newops)
        return ops

    def check_tz_compatibility(self, cn, tree):
        """check that series are timezone-compatible
        """
        metamap = {}
        helper.find_tz(self, cn, tree, metamap)

        first_tzaware = None
        if metamap:
            first_tzaware = next(iter(metamap.values()))
            for (name, path), tzaware in metamap.items():
                if first_tzaware != tzaware:
                    raise ValueError(
                        f'Formula `{name}` has tzaware vs tznaive series:'
                        f'{",".join("`%s:%s`" % (k, helper.tzlabel(v)) for k, v in metamap.items())}'
                    )

        return first_tzaware

    def formula_stats(self, cn, name):
        autos = helper.find_autos(cn, self, name)
        nodes = helper.scan_descendant_nodes(cn, self, name)
        nodes.update({'autotrophics': autos})
        return nodes

    @tx
    def register_dependents(self, cn, name, tree):
        cn.execute(
            f'delete from "{self.namespace}".dependent where '
            f'sid in (select id from "{self.namespace}".registry '
            f'        where name = %(name)s)',
            name=name
        )
        for dep in self.find_series(cn, tree):
            if self.type(cn, dep) != 'formula':
                continue
            cn.execute(
                f'insert into "{self.namespace}".dependent '
                f'(sid, needs) '
                f'values ('
                f' (select id from "{self.namespace}".registry where name = %(name)s),'
                f' (select id from "{self.namespace}".registry where name = %(dep)s)'
                f') on conflict do nothing',
                name=name,
                dep=dep
            )

    @tx
    def dependents(self, cn, name, direct=False):
        all_formulas = [
            name
            for name, type in self.list_series(cn).items()
            if type == 'formula'
        ]
        formulas_find = defaultdict(list)
        for formula_name in all_formulas:
            formula = self.formula(cn, formula_name)
            if 'findseries' in formula:
                rewritten_tree = helper.replace_findseries(
                    cn, self, parse(formula)
                )
                components = self.find_series(cn, rewritten_tree)
                for comp in components:
                    formulas_find[comp].append(formula_name)

        return self._dependents(
            cn,
            name,
            direct=direct,
            formulas_find=formulas_find,
        )

    def _dependents(self, cn, name, direct=False, formulas_find={}):
        deps = [
            n for n, in cn.execute(
                f'select f.name '
                f'from "{self.namespace}".registry as f, '
                f'     "{self.namespace}".registry as f2,'
                f'     "{self.namespace}".dependent as d '
                f'where f.id = d.sid and '
                f'      d.needs = f2.id and '
                f'      f2.name = %(name)s',
                name=name
            ).fetchall()
        ]
        dynamic_deps = formulas_find.get(name, [])
        deps = deps + dynamic_deps
        if direct:
            return sorted(set(deps))

        for dname in deps[:]:
            deps.extend(
                self._dependents(
                    cn,
                    dname,
                    formulas_find=formulas_find
                )
            )
        return sorted(set(deps))

    @tx
    def register_formula(self, cn, name, formula, reject_unknown=True):
        assert isinstance(name, str), 'The name must be a string'
        name = name.strip()
        assert len(name), 'The new name must contain non whitespace items'
        exists = self.exists(cn, name)

        if exists and self.type(cn, name) == 'primary':
            raise TypeError(
                f'primary series `{name}` cannot be overriden by a formula'
            )

        # basic syntax check
        tree = parse(formula)
        helper.validate(tree)
        # this normalizes the formula
        formula = serialize(tree)

        # bad operators
        operators = self.find_operators(cn, tree)
        badoperators = [
            op
            for op, func in operators.items()
            if func is None
        ]
        if badoperators:
            raise ValueError(
                f'Formula `{name}` refers to unknown operators '
                f'{", ".join("`%s`" % o for o in badoperators)}'
            )

        # type checking
        i = interpreter.Interpreter(cn, self, {})
        rtype = types.typecheck(tree, env=i.env)
        if not types.sametype(rtype, pd.Series):
            raise TypeError(
                f'formula `{name}` must return a `Series`, not `{rtype.__name__}`'
            )

        # build metadata & check compat
        seriesmeta = self.find_metas(cn, tree)
        if not all(seriesmeta.values()) and reject_unknown:
            badseries = [k for k, v in seriesmeta.items() if not v]
            raise ValueError(
                f'Formula `{name}` refers to unknown series '
                f'{", ".join("`%s`" % s for s in badseries)}'
            )

        tzaware = self.check_tz_compatibility(cn, tree)
        etree = self._expanded_formula(cn, formula, remote=False)
        ch = hashlib.sha1(
            serialize(etree).encode()
        ).hexdigest()

        if tzaware is None:
            # bad situation ... we should stop accepting references to
            # unknown series and provide a batch registration api
            # point instead
            L.warning('formula %s has no tzaware info (will be broken)', name)

        coremeta = self.default_internal_meta(tzaware)
        meta = self.internal_metadata(cn, name) or {}
        meta = dict(meta, **coremeta)
        meta['formula'] = formula
        meta['contenthash'] = ch
        self._register_formula(cn, name, meta, exists)
        self.register_dependents(cn, name, tree)

    def _register_formula(self, cn, name, internal_meta, exists):
        if exists:
            # update
            cn.execute(
                f'update "{self.namespace}".registry '
                'set internal_metadata = %(imeta)s '
                'where name=%(name)s',
                name=name,
                imeta=json.dumps(internal_meta)
            )
            return

        # creation
        cn.execute(
            f'insert into "{self.namespace}".registry '
            '(name, internal_metadata, metadata) '
            'values (%s, %s, %s) '
            'returning id',
            name,
            json.dumps(internal_meta),
            json.dumps({})
        )

    def live_content_hash(self, cn, name):
        return hashlib.sha1(
            serialize(
                self._expanded_formula(
                    cn,
                    self.formula(cn, name),
                    remote=False  # bw compat
                )
            ).encode()
        ).hexdigest()

    def default_internal_meta(self, tzaware):
        if tzaware:
            return {
                'tzaware': True,
                'index_type': 'datetime64[ns, UTC]',
                'value_type': 'float64',
                'index_dtype': '|M8[ns]',
                'value_dtype': '<f8'
            }
        return {
            'index_dtype': '<M8[ns]',
            'index_type': 'datetime64[ns]',
            'tzaware': False,
            'value_dtype': '<f8',
            'value_type': 'float64'
        }

    def content_hash(self, cn, name):
        return cn.execute(
            f'select internal_metadata->\'contenthash\' '
            f'from "{self.namespace}".registry '
            f'where name=%(name)s',
            name=name
        ).scalar()

    def formula(self, cn, name):
        return cn.execute(
            f'select internal_metadata->\'formula\' '
            f'from "{self.namespace}".registry '
            f'where name = %(name)s',
            name=name
        ).scalar()

    def list_series(self, cn):
        series = super().list_series(cn)
        sql = (
            f'select name from "{self.namespace}".registry '
            'where internal_metadata->\'formula\' is not null'
        )
        series.update({
            name: 'formula'
            for name, in cn.execute(sql)
        })
        return series

    def type(self, cn, name):
        if self.formula(cn, name):
            return 'formula'

        return super().type(cn, name)

    def exists(self, cn, name):
        return super().exists(cn, name) or bool(self.formula(cn, name))

    @tx
    def delete(self, cn, name):
        if self.type(cn, name) != 'formula':
            return super().delete(cn, name)

        cn.execute(
            f'delete from "{self.namespace}".registry '
            'where name = %(name)s',
            name=name
        )
        if self.patch.exists(cn, name):
            self.patch.delete(cn, name)

    def update(self, cn, updatets, name, author, **k):
        if self.type(cn, name) == 'formula':
            # that was premature, and will return in a later version
            # return self.patch.update(cn, updatets, name, author, **k)
            raise ValueError(f'`{name}` is a formula, it cannot be updated')
        return super().update(cn, updatets, name, author, **k)

    @tx
    def get(self, cn, name, **kw):
        formula = self.formula(cn, name)
        if formula:
            if helper.has_loop(
                    cn,
                    name,
                    parse(formula),
                    interpreter.Interpreter(cn, self, kw)):
                return empty_series(self.tzaware(cn, name))

            ts = self.eval_formula(cn, formula, **kw)
            if ts is not None:
                ts.name = name
            if self.patch.exists(cn, name):
                ts_patch = self.patch.get(cn, name, **kw)
                if not len(ts_patch):
                    return ts
                ts = patch(ts, ts_patch)
            # preserve the options
            opts = getattr(ts, 'options', {})
            tzaware = self.tzaware(cn, name)
            ts = ts.loc[
                compatible_date(tzaware, kw.get('from_value_date')):
                compatible_date(tzaware, kw.get('to_value_date'))
            ]
            ts.options = opts
            return ts

        ts = super().get(cn, name, **kw)
        if ts is None and self.othersources:
            ts = self.othersources.get(
                name, **kw
            )

        return ts

    def interval(self, cn, name, notz=False):
        formula = self.formula(cn, name)
        if not formula:
            return super().interval(cn, name, notz)

        tree = parse(formula)
        series = self.find_series(cn, tree)
        tz = 'utc' if self.tzaware(cn, name) else None
        mindate = pd.Timestamp('2262-1-1', tz=tz)
        maxdate = pd.Timestamp('1677-09-23', tz=tz)

        for name in series:
            ival = self.interval(cn, name, notz)
            mindate = min(ival.left, mindate)
            maxdate = max(ival.right, maxdate)

        return pd.Interval(mindate, maxdate)

    def eval_formula(self, cn, formula, **kw):
        i = kw.get('__interpreter__') or interpreter.Interpreter(cn, self, kw)
        ts = i.evaluate(
            self._expanded_formula(cn, formula, display=False, qargs=kw)
        )
        return ts

    def _expanded_formula(self, cn, formula, stopnames=(), level=-1,
                          display=True, remote=False, qargs=None):
        exp = helper.expanded(
            self,
            cn,
            parse(formula),
            stopnames=stopnames,
            scopes=qargs is not None,
            remote=remote,
            level=level
        )
        if not display:
            return helper.inject_toplevel_bindings(
                exp,
                qargs or {}
            )
        return exp

    def expanded_formula(self, cn, name, stopnames=(), display=True, remote=False,
                         level=-1, **kw):
        formula = self.formula(cn, name)
        if formula is None:
            return

        tree = self._expanded_formula(cn, formula, stopnames, level, display, remote, kw)
        if tree is None:
            return

        return serialize(tree)

    def depth(self, cn, name):
        formula = self.formula(cn, name)
        if formula is None:
            return

        return helper.depth(self, cn, parse(formula))

    @tx
    def iter_revisions(
            self, cn, name,
            from_value_date=None,
            to_value_date=None,
            from_insertion_date=None,
            to_insertion_date=None,
            **kw):
        idates = self.insertion_dates(
            cn, name,
            from_insertion_date=from_insertion_date,
            to_insertion_date=to_insertion_date
        )
        for idate in idates:
            yield idate, self.get(
                cn, name,
                revision_date=idate,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )

    def _custom_idates_sites(self, cn, tree):
        return [
            call
            for sname in IDATES
            for call in self.find_callsites(cn, sname, tree)
        ]

    @tx
    def insertion_dates(self, cn, name,
                        from_insertion_date=None,
                        to_insertion_date=None,
                        from_value_date=None,
                        to_value_date=None,
                        **kw):
        if self.type(cn, name) != 'formula':
            return super().insertion_dates(
                cn, name,
                from_insertion_date=from_insertion_date,
                to_insertion_date=to_insertion_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                **kw
            )

        formula = self.formula(cn, name)
        tree = parse(formula)
        series = self.find_series(cn, tree)
        allrevs = []
        for name in series:
            if not self.exists(cn, name):
                if self.othersources:
                    allrevs += self.othersources.insertion_dates(
                        name,
                        from_insertion_date=from_insertion_date,
                        to_insertion_date=to_insertion_date,
                        from_value_date=from_value_date,
                        to_value_date=to_value_date,
                        **kw
                    )
                continue
            if self.formula(cn, name):
                allrevs += self.insertion_dates(
                    cn, name,
                    from_insertion_date=from_insertion_date,
                    to_insertion_date=to_insertion_date,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    **kw
                )
            else:
                allrevs += [
                    idate
                    for _id, idate in self._revisions(
                            cn, name,
                            from_insertion_date=from_insertion_date,
                            to_insertion_date=to_insertion_date,
                            from_value_date=from_value_date,
                            to_value_date=to_value_date
                    )]

        # autotrophic operators
        isites = self._custom_idates_sites(cn, tree)
        for site in isites:
            fname = site[0]
            idates_func = IDATES[fname]
            revs = idates_func(
                cn, self, site,
                from_insertion_date,
                to_insertion_date,
                from_value_date,
                to_value_date
            )
            if revs:
                allrevs += revs

        # /auto

        return sorted(set(allrevs))

    @tx
    def latest_insertion_date(self, cn, name):
        formula = self.formula(cn, name)
        if formula:
            return self.insertion_dates(cn, name)[-1]

        return super().latest_insertion_date(cn, name)

    @tx
    def first_insertion_date(self, cn, name):
        formula = self.formula(cn, name)
        if formula:
            return self.insertion_dates(cn, name)[0]

        return super().first_insertion_date(cn, name)

    @tx
    def staircase(self, cn, name, delta,
                  from_value_date=None,
                  to_value_date=None):
        formula = self.formula(cn, name)
        if formula:
            if interpreter.has_compatible_operators(
                    cn, self,
                    parse(formula),
                    self.fast_staircase_operators):
                # go fast
                return self.get(
                    cn, name,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date,
                    __interpreter__=interpreter.FastStaircaseInterpreter(
                        cn, self,
                        {'from_value_date': from_value_date,
                         'to_value_date': to_value_date},
                        delta
                    )
                )

        return super().staircase(
            cn, name, delta,
            from_value_date,
            to_value_date
        )

    @tx
    def log(self, cn, name, **kw):
        if self.formula(cn, name):
            if self.cache.exists(cn, name):
                return self.cache.log(
                    cn, name, **kw
                )
            return []

        return super().log(cn, name, **kw)

    @tx
    def rename(self, cn, oldname, newname, propagate=True):
        assert isinstance(oldname, str), 'The old name must be a string'
        assert isinstance(newname, str), 'The new name must be a string'
        newname = newname.strip()
        assert len(newname), 'The new name must contain non whitespace items'

        if not propagate:
            super().rename(cn, oldname, newname)
            if self.patch.exists(cn, oldname):
                self.patch.rename(cn, oldname, newname)
            return

        # read all formulas and parse them ...
        formulas = cn.execute(
            f'select name, internal_metadata->\'formula\' '
            f'from "{self.namespace}".registry'
        ).fetchall()
        errors = []

        def edit(tree, oldname, newname):
            newtree = []
            series = False
            for node in tree:
                if isinstance(node, list):
                    newtree.append(edit(node, oldname, newname))
                    continue
                if node == 'series':
                    series = True
                    newtree.append(node)
                    continue
                elif node == oldname and series:
                    node = newname
                newtree.append(node)
                series = False
            return newtree

        rewritten = {}
        for fname, text in formulas:
            tree = parse(text)
            series = self.find_series(
                cn,
                tree
            )
            if newname in series:
                errors.append(fname)
                continue
            if oldname not in series:
                continue

            rewritten[fname] = serialize(edit(tree, oldname, newname))

        if errors:
            raise ValueError(
                f'new name is already referenced by `{",".join(errors)}`'
            )

        for fname, text in rewritten.items():
            # updating using jsonb_set is such a PITA ... we for now
            # prefer to be slightly more expensive
            self.update_internal_metadata(cn, fname, {'formula': text})

        if self.patch.exists(cn, oldname):
            self.patch.rename(cn, oldname, newname)
        super().rename(cn, oldname, newname)


    # find override

    _find_items = ['name', 'internal_metadata->\'formula\'']

    def _finish_find(self, cn, q, meta, source):
        if not meta:
            return [
                ts(name, source=source, kind='formula' if formula else 'primary')
                for name, formula in q.do(cn).fetchall()
            ]

        return [
            ts(name, imeta, umeta, source, kind='formula' if formula else 'primary')
            for name, formula, imeta, umeta in q.do(cn).fetchall()
        ]

    # groups

    @tx
    def group_type(self, cn, name):
        imeta = self.group_internal_metadata(cn, name)
        if not imeta:
            return 'primary'
        if 'formula' in imeta:
            return 'formula'
        if 'bindings' in imeta:
            return 'bound'
        return 'primary'

    def check_group_tz_compatibility(self, cn, tree):
        """check that groups are timezone-compatible
        """

        def find_meta(tree, tzstatus, path=()):
            op = tree[0]
            path = path + (op,)
            metas = GMETAS.get(op)
            if metas:
                for name, metadata in metas(cn, self, tree).items():
                    tzaware = metadata['tzaware'] if metadata else None
                    # no naive operator (yet) so we don't
                    # have to handle it
                    tzstatus[(name, path)] = tzaware
            for item in tree:
                if isinstance(item, list):
                    find_meta(item, tzstatus, path)

        metamap = {}
        find_meta(tree, metamap)
        if not metamap:
            return {}

        def tzlabel(status):
            if status is None:
                return 'unknown'
            return 'tzaware' if status else 'tznaive'

        first_tzaware = next(iter(metamap.values()))
        for (name, path), tzaware in metamap.items():
            if first_tzaware != tzaware:
                raise ValueError(
                    f'Formula `{name}` has tzaware vs tznaive series:'
                    f'{",".join("`%s:%s`" % (k, tzlabel(v)) for k, v in metamap.items())}'
                )
        return first_tzaware

    @tx
    def register_group_formula(self, cn,
                               name, formula):
        exists = self.group_exists(cn, name)
        if exists and self.group_type(cn, name) != 'formula':
            raise TypeError(
                f'cannot register formula `{name}`: already a `{self.group_type(cn, name)}`'
            )
        # basic syntax check
        tree = parse(formula)
        formula = serialize(tree)

        # type checking
        i = interpreter.GroupInterpreter(cn, self, {})
        rtype = types.typecheck(tree, env=i.env)
        if not types.sametype(rtype, pd.DataFrame):
            raise TypeError(
                f'formula `{name}` must return a `DataFrame`, not `{rtype.__name__}`'
            )

        # build metadata & check compat
        seriesmeta = self.find_metas(cn, tree)
        if not all(seriesmeta.values()):
            badseries = [k for k, v in seriesmeta.items() if not v]
            raise ValueError(
                f'Formula `{name}` refers to unknown series '
                f'{", ".join("`%s`" % s for s in badseries)}'
            )

        tzaware = self.check_group_tz_compatibility(cn, tree)
        coremeta = self.default_internal_meta(tzaware)
        coremeta['formula'] = formula

        if exists:
            # update
            cn.execute(
                f'update "{self.namespace}".group_registry '
                'set internal_metadata = %(imeta)s '
                'where name=%(name)s',
                name=name,
                imeta=json.dumps(coremeta)
            )
            return

        sql = (
            f'insert into "{self.namespace}".group_registry '
            '             (name, internal_metadata, metadata) '
            'values (%(name)s, %(imeta)s, %(meta)s) '
        )
        cn.execute(
            sql,
            name=name,
            imeta=json.dumps(coremeta),
            meta=json.dumps({})
        )

    @tx
    def group_formula(self, cn, groupname):
        res = cn.execute(
            f'select internal_metadata->\'formula\' '
            f'from "{self.namespace}".group_registry '
            f'where name = %(name)s',
            name=groupname
        )
        return res.scalar()

    @tx
    def list_groups(self, cn):
        return {
            name: 'formula' if formula else 'bound' if bindings else 'primary'
            for name, formula, bindings in cn.execute(
                f'select name, '
                '        internal_metadata->\'formula\', '
                '        internal_metadata->\'bindings\' '
                f'from "{self.namespace}".group_registry'
            ).fetchall()
        }

    @tx
    def group_get(self, cn, groupname,
                  revision_date=None,
                  from_value_date=None,
                  to_value_date=None):
        # case of formula
        formula = self.group_formula(cn, groupname)
        if formula:
            interp = interpreter.GroupInterpreter(
                cn, self,
                dict(
                    revision_date=revision_date,
                    from_value_date=from_value_date,
                    to_value_date=to_value_date
                )
            )
            df = self.eval_formula(
                cn, formula,
                revision_date=revision_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                __interpreter__=interp
            )
            if df.index.dtype != 'object':
                df.columns = [str(col) for col in df.columns]
            if df.index.name:
                df.index.name = None
            return df

        bindinfo = self.bindings_for(
            cn, groupname
        )
        if bindinfo:
            return self._hijacked_formula(
                cn,
                bindinfo[0],
                bindinfo[1],
                revision_date=revision_date,
                from_value_date=from_value_date,
                to_value_date=to_value_date
            )

        return super().group_get(
            cn,
            groupname,
            revision_date=revision_date,
            from_value_date=from_value_date,
            to_value_date=to_value_date
        )

    def find_groups_and_series(self, cn, tree):
        op = tree[0]
        super_finder = dict(GFINDERS, **FINDERS)
        finder = super_finder.get(op)
        seriestree = finder(cn, self, tree) if finder else {}
        for item in tree:
            if isinstance(item, list):
                seriestree.update(
                    self.find_groups_and_series(cn, item)
                )
        return seriestree

    @tx
    def group_insertion_dates(self, cn, name, **bounds):
        if not self.group_exists(cn, name):
            return None
        formula = self.group_formula(cn, name)

        if formula:
            tree = parse(formula)
            groups_and_series = self.find_groups_and_series(cn, tree)
            allrevs = []
            for name, info in groups_and_series.items():
                operator_name = str(info[0])
                if operator_name == 'group':
                    if not self.group_exists(cn, name):
                        continue
                    allrevs += self.group_insertion_dates(cn, name, **bounds)
                elif operator_name == 'series':
                    if not self.exists(cn, name):
                        continue
                    allrevs += self.insertion_dates(cn, name, **bounds)
                elif operator_name in GAUTO:
                    idates_func = GIDATES[operator_name]
                    allrevs += idates_func(cn, self, tree, **bounds)

            return sorted(set(allrevs))

        bindinfo = self.bindings_for(
            cn, name
        )

        if bindinfo:
            # hijacked formula
            # we only return the idates when all the groups are present
            # This API point merely provide the definition period
            # where the hijacking is fully usable (and backtestable)
            seriesname, bindings = bindinfo

            tree = helper.expanded(
                self,
                cn,
                parse(self.formula(cn, seriesname)),
                shownames=bindings['series'].values,
                scopes=False
            )
            series = self.find_series(cn, tree)

            # let's only keep the groups that are really used
            # users like to provide mappings that contain
            # other (irrelevant) groups
            groups_names = {
                gname for sname, gname in
                zip(
                    bindings['series'],
                    bindings['group']
                )
                if sname in series
            }

            all_idates = [
                self.group_insertion_dates(cn, gn, **bounds)
                for gn in groups_names
            ]
            first_date_with_all_presents = max(
                min(dates)
                for dates in all_idates
            )
            all_idates_bounded = [
                {
                    idate
                    for idate in idates
                    if idate >= first_date_with_all_presents
                 }
                for idates in all_idates
            ]
            final_dates = set()
            for dates in all_idates_bounded:
                final_dates = final_dates.union(dates)
            return sorted(final_dates)

        # primaries
        return super().group_insertion_dates(cn, name, **bounds)


    @tx
    def group_history(self, cn, name,
                      from_value_date=None,
                      to_value_date=None,
                      from_insertion_date=None,
                      to_insertion_date=None):
        idates = self.group_insertion_dates(
            cn,
            name,
            from_insertion_date=from_insertion_date,
            to_insertion_date=to_insertion_date
        )
        if idates is None:
            return None
        if not len(idates):
            return {}
        history = {}
        for idate in idates:
            history[idate] = self.group_get(
                cn,
                name,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                revision_date=idate
            )
        return history

    # group formula binding

    @tx
    def register_formula_bindings(self, cn, groupname, formulaname, binding, nockeck=False):
        if set(binding.columns) != {'series', 'group', 'family'}:
            raise ValueError(
                'bindings must have `series` `groups` and `family` columns'
            )

        if self.type(cn, formulaname) != 'formula':
            raise ValueError(f'`{formulaname}` is not a formula')

        if not nockeck:
            grtzstate = []
            for gname in binding.group:
                assert self.group_exists(cn, gname), f'Group `{gname}` does not exist.'
                grtzstate.append(
                    (gname, self.group_internal_metadata(cn, gname)['tzaware'])
                )
            tstzstate = []
            for sname in binding.series:
                assert self.exists(cn, sname), f'Series `{sname}` does not exist.'
                tstzstate.append(
                    (sname, self.internal_metadata(cn, sname)['tzaware'])
                )
            for (gname, gtz), (sname, stz) in zip(grtzstate, tstzstate):
                assert gtz == stz, f'Series `{sname}` and group `{gname}` must be tz-compatible.'

        if not len(binding):
            raise ValueError(f'formula `{formulaname}` has an empty binding')

        gtype = self.group_type(cn, groupname)
        exists = self.group_exists(cn, groupname)
        if exists and gtype != 'bound':
            raise ValueError(f'cannot bind `{groupname}`: already a {gtype}')

        if exists:
            imeta = self.group_internal_metadata(cn, groupname)
            imeta['bindings'] = binding.to_json(orient='records')
            imeta['boundseries'] = formulaname
            cn.execute(
                f'update "{self.namespace}"."group_registry" '
                f'set internal_metadata = %(imeta)s '
                f'where name = %(gname)s',
                gname=groupname,
                imeta=json.dumps(imeta)
            )
            return

        coremeta = self.internal_metadata(cn, formulaname)
        coremeta.pop('formula')
        coremeta.pop('contenthash')
        coremeta['bindings'] = binding.to_json(orient='records')
        coremeta['boundseries'] = formulaname
        cn.execute(
            f'insert into "{self.namespace}"."group_registry" '
            f'            (name, internal_metadata, metadata) '
            'values (%(gname)s, %(imeta)s, %(meta)s)',
            gname=groupname,
            imeta=json.dumps(coremeta),
            meta=json.dumps({})
        )

    @tx
    def bindings_for(self, cn, groupname):
        res = cn.execute(
            f'select internal_metadata->\'boundseries\', '
            f'       internal_metadata->\'bindings\' '
            f'from "{self.namespace}".group_registry '
            'where name = %(gname)s',
            gname=groupname
        )
        sname_binding = res.fetchone()
        if sname_binding is None or sname_binding[0] is None:
            return
        binding = pd.DataFrame(
            json.loads(
                sname_binding[1]
            )
        )
        return sname_binding[0], binding

    @tx
    def get_bound_group(self, cn, name, binding,
                        from_value_date=None,
                        to_value_date=None,
                        revision_date=None):
        m = binding['series'] == name
        if sum(m) == 0:
            # unbound series
            return None, None

        assert sum(m) == 1
        groupname = binding.loc[m, 'group'].iloc[0]
        family = binding.loc[m, 'family'].iloc[0]
        return (
            self.group_get(
                cn,
                groupname,
                from_value_date=from_value_date,
                to_value_date=to_value_date,
                revision_date=revision_date,
            ),
            family
        )

    @tx
    def _hijacked_formula(self, cn, name, binding,
                          from_value_date=None,
                          to_value_date=None,
                          revision_date=None):
        # find all the series in the formula that are referenced in
        # the binding since the series can be anywhere in the
        # dependencies - we use exanded_formula using (shownames) to
        # limit the expansion to the minimum necessary to show such names.
        # This semi-expanded formula will be
        # used later in the interpretor
        tree = helper.expanded(
            self,
            cn,
            parse(self.formula(cn, name)),
            shownames=binding['series'].values,
            scopes=True,
        )
        new_tree = helper.inject_toplevel_bindings(
            tree,
            {
                'revision_date': revision_date,
                'from_value_date': from_value_date,
                'to_value_date': to_value_date,
            }
        )
        series = self.find_series(cn, new_tree)
        formula = serialize(new_tree)

        groupmap = defaultdict(dict)
        for sname in series:
            df, family = self.get_bound_group(
                cn,
                sname,
                binding,
                from_value_date,
                to_value_date,
                revision_date
            )
            if family is not None:
                assert df is not None
                groupmap[family][sname] = df

        assert groupmap

        bi = interpreter.BridgeInterpreter(
            cn, self, {},
            groups=groupmap,
            binding=binding,
        )

        # build scenarios combinations
        possible_values = []
        families = []
        for ens_name, sub in groupmap.items():
            # we may have several groups there
            # but they all have the same column names
            # and this is what we want
            df = sub[list(sub.keys())[0]]
            possible_values.append(df.columns.to_list())
            families.append(ens_name)

        # combination is a list of dict tha contain a unqiue scenario...
        # [{ens0 = 'scenario0', ens1 = 'scenario4'}, ... ]
        combinations = []
        for comb in itertools.product(*possible_values):
            combination = {}
            for idx, values in enumerate(comb):
                combination.update({families[idx]: values})
            combinations.append(combination)

        columns = []
        for combination in combinations:
            columns.append(bi.g_evaluate(formula, combination))

        return pd.concat(columns, axis=1)
