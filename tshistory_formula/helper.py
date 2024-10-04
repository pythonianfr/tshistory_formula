import inspect
import queue
import threading
from concurrent.futures import _base

import pandas as pd

from psyl.lisp import (
    buildargs,
    Keyword,
    parse,
    serialize,
    Symbol
)

from tshistory import search


from tshistory_formula.registry import (
    FUNCS,
    METAS,
    ARGSCOPES,
    AUTO,
)


class seriesname(str):
    pass


class BadKeyword(Exception):
    pass


def validate(tree):
    if not isinstance(tree, list):
        return

    kw = None
    for item in tree:
        validate(item)
        if isinstance(item, Keyword):
            if kw is not None:
                # two consecutive keywords ?
                raise BadKeyword(f'keyword `#:{kw}` not followed by a value')
            kw = item
        else:
            kw = None
    if kw is not None:
        raise BadKeyword(f'keyword `#:{kw}` not followed by a value')


def rename_operator(tree, oldname, newname):
    if not isinstance(tree, list):
        return tree

    op = tree[0]
    if op == Symbol(oldname):
        tree[0] = Symbol(newname)

    return [
        rename_operator(item, oldname, newname)
        for item in tree
    ]


def fix_holidays(tree):
    if not isinstance(tree, list):
        return tree

    op = tree[0]
    if op == 'holidays':
        # insert positionnally fromdate and todate
        # with reasonnable constants
        tree.insert(2, [Symbol('date'), "2020-1-1"])
        tree.insert(3, [Symbol('now')])

    return [
        fix_holidays(item)
        for item in tree
    ]


def extract_auto_options(tree):
    options = []
    optnames = ('fill', 'limit', 'weight')

    keyword = None
    for item in tree:
        if keyword is not None:
            options.append(item)
            keyword = None
            continue
        if item in optnames:
            options.append(item)
            keyword = item

    return options


def zonename(tzinfo):
    if pd.__version__.startswith('1.'):
        return tzinfo.zone
    return str(tzinfo).upper()


def inject_toplevel_bindings(tree, qargs):
    top = [Symbol('let')]
    for attr in ('revision_date', 'from_value_date', 'to_value_date'):
        val = qargs.get(attr)
        # naive must remain naive
        # to allow the naive operator to do its transform
        tzone = zonename(val.tzinfo) if val and val.tzinfo else Symbol('nil')
        top += [
            Symbol(attr),
            [Symbol('date'), val.isoformat(), tzone]
            if val else Symbol('nil')
        ]

    top.append(tree)
    return top


def has_names(tsh, cn, tree, names, stopnames):
    if tree[0] == 'series':
        name = tree[1]
        if name in names:
            return True

        if tsh.type(cn, name) == 'formula':
            return has_names(
                tsh,
                cn,
                expanded(tsh, cn, tree, stopnames=names, scopes=False),
                names,
                stopnames
            )

    for item in tree[1:]:
        if isinstance(item, list):
            if has_names(
                tsh,
                cn,
                expanded(tsh, cn, item, stopnames=names, scopes=False),
                names,
                stopnames,
            ):
              return True

    return False


def formula(tsh, cn, name, remote):
    f = tsh.formula(cn, name)
    if f is not None:
        return f

    if remote and tsh.othersources:
        f = tsh.othersources.formula(name)
        if f is not None:
            return f

    return None


def expanded(
        tsh,
        cn,
        tree,
        stopnames=(),
        shownames=(),
        scoped=None,
        scopes=True,
        remote=False,
        level=-1
):
    # handle scoped parameter (internal memo)
    scoped = set() if scoped is None else scoped

    # base case: check the current operation
    op = tree[0]

    if scopes and op in ARGSCOPES:
        if id(tree) not in scoped:
            # we need to avoid an infinite recursion
            # as the new tree contains the old ...
            scoped.add(id(tree))
            rewriter = ARGSCOPES[op]
            return rewriter(
                expanded(
                    tsh,
                    cn,
                    tree,
                    stopnames,
                    shownames,
                    scoped,
                    scopes,
                    remote=remote
                )
            )

    if op == 'series':
        metas = METAS.get(op)
        seriesmeta = metas(cn, tsh, tree)
        name, _ = seriesmeta.popitem()
        if len(shownames) and not has_names(tsh, cn, tree, shownames, ()):
            return tree
        if name in shownames:
            return tree
        if name in stopnames:
            return tree
        form = formula(tsh, cn, name, remote)
        if form and level:
            options = extract_auto_options(tree)
            if not options:
                return expanded(
                    tsh,
                    cn,
                    parse(form),
                    stopnames,
                    shownames,
                    scopes=scopes,
                    remote=remote,
                    level=level-1
                )
            return [
                Symbol('options'),
                expanded(
                    tsh,
                    cn,
                    parse(form),
                    stopnames,
                    shownames,
                    scopes=scopes,
                    remote=remote,
                    level=level-1
                ),
            ] + options

    # expand findseries
    tree_find = replace_findseries(cn, tsh, tree)
    if tree_find != tree and level:
        return expanded(
            tsh,
            cn,
            tree_find,
            stopnames,
            shownames,
            scopes=scopes,
            remote=remote,
            level=level - 1
        )

    newtree = []
    for item in tree:
        if isinstance(item, list):
            newtree.append(
                expanded(
                    tsh,
                    cn,
                    item,
                    stopnames,
                    shownames,
                    scopes=scopes,
                    remote=remote,
                    level=level
                )
            )
        else:
            newtree.append(item)
    return newtree


def depth(
        tsh,
        cn,
        tree,
):
    # base case: check the current operation
    op = tree[0]
    if op == 'series':
        metas = METAS.get(op)
        seriesmeta = metas(cn, tsh, tree)
        name, _ = seriesmeta.popitem()
        if tsh.type(cn, name) == 'formula':
            formula = tsh.formula(cn, name)
            return depth(
                tsh,
                cn,
                parse(formula),
            ) + 1
        elif not tsh.exists(cn, name):
            # ah, not a local formula,
            # we must cross the chasm
            if tsh.othersources is not None:
                d = tsh.othersources.formula_depth(name)
                if d is not None:
                    return d + 1

        return 0

    elif op == 'findseries':
        query_tree = tree[1]
        kwargs = buildargs(tree)[-1]
        tree_find = substitute_findseries(
            cn,
            tsh,
            query_tree,
            kwargs=kwargs,
        )
        if not len(tree_find):
            return 1
        return 1 + depth(tsh, cn, tree_find)

    depths = []
    for item in tree:
        if isinstance(item, list):
            depths.append(
                depth(tsh, cn, item)
            )
    return max(depths) if depths else 0


def has_loop(cn, name, parsed, iterp):

    def _hasloop(tree):
        op = tree[0]
        if op == 'findseries':
            # evaluate and see
            itree = inject_toplevel_bindings(
                [Symbol('findnames'), tree[1]], {}
            )
            names = iterp.evaluate(itree)
            if name in names:
                return True

        for item in tree:
            if isinstance(item, list):
                if _hasloop(item):
                    return True

        return False

    return _hasloop(parsed)



# stats stuff

def update_dict_list(ds0, ds1):
    for k, vs in ds1.items():
        if k in ds0:
            ds0[k].extend(ds1[k])
        else:
            ds0[k] = ds1[k]
    return ds0


def enumlist(alist):
    return [(elt, alist.count(elt)) for elt in sorted(set(alist))]


def sort_dict_list(dl):
    return {k : enumlist(dl[k]) for k in sorted(dl)}


def count_values(d):
    return {(k, len(v), len(set(v))):v for k,v in d.items()}


def find_autos(cn, tsh, name):
    return sort_dict_list(
        count_values(
            _find_autos(
                cn,
                tsh,
                tsh._expanded_formula(cn, tsh.formula(cn, name))
            )
        )
    )


def _find_autos(cn, tsh, tree):
    autos = {}
    auto_without_series = AUTO.copy()
    auto_without_series.pop('series')
    for item in tree:
        if isinstance(item, list):
            autos = update_dict_list(
                autos,
                _find_autos(cn, tsh, item),
            )
        elif item in auto_without_series:
            update_dict_list(
                autos,
                {str(item): [serialize(tree)]},
            )
    return autos


def scan_descendant_nodes(cn, tsh, name):
    primaries = []
    named_nodes = []
    depths = []

    def explore_tree(cn, tsh, tree, depth):
        depth += 1
        depths.append(depth)
        lseries = tsh.find_series(cn, tree)
        for series in lseries:
            formula = tsh.formula(cn, series)
            if not formula:
                # could be the forged name of an autotrophic operator
                if tsh.exists(cn, series):
                    primaries.append(series)
                continue
            named_nodes.append(series)
            subtree = parse(formula)
            explore_tree(cn, tsh, subtree, depth)

    tree = parse(tsh.formula(cn, name))
    explore_tree(cn, tsh, tree, depth=0)
    return {
        ('named-nodes', len(named_nodes), len(set(named_nodes))) :
            enumlist(named_nodes),
        'degree': max(depths),
        ('primaries', len(primaries), len(set(primaries))):
            enumlist(primaries),
    }


# findseries

def replace_findseries(cn, tsh, tree):
    newtree = []
    for elt in tree:
        if isinstance(elt, list):
            if elt[0] == Symbol('findseries'):
                kwargs = buildargs(elt)[-1]
                query_tree = elt[1]
                # This concatenation is key to properly handle
                #   (add (findseries ...)
                # being transformed as
                #   (add (series ...) (series ...))
                # rather than
                #   (add ((series ...) (series ...)))
                newtree += substitute_findseries(
                    cn,
                    tsh,
                    query_tree,
                    kwargs,
                )
            else:
                newtree.append(
                    replace_findseries(cn, tsh, elt)
                )
        else:
            newtree.append(elt)
    return newtree


def substitute_findseries(cn, tsh, tree, kwargs):
    # protection against circular import
    from tshistory_formula.interpreter import Interpreter

    naive = kwargs.get(Keyword('naive'), False)
    fill_option = kwargs.get(Keyword('fill'), None)
    i = Interpreter(cn, tsh, kwargs)
    query_search = i.evaluate(tree)
    if naive:
        query_search = search.and_(
            search.not_(
                search.tzaware()
            ),
            query_search
        )
    else:
        query_search = search.and_(
            search.tzaware(),
            query_search
        )
    names = tsh.find(cn, query_search)

    if fill_option is None:
        return [
            [Symbol('series'), name]
            for name in names
        ]

    return [
        [Symbol('series'), name, Keyword('fill'), fill_option]
        for name in names
    ]


# signature building

def name_of_expr(expr):
    return _name_from_signature_and_args(*_extract_from_expr(expr))


def _name_from_signature_and_args(name, func, a, kw):
    sig = inspect.signature(func)
    out = [name]
    for idx, (pname, param) in enumerate(sig.parameters.items()):
        if pname.startswith('__'):
            continue
        if param.default is inspect._empty:
            # almost pure positional
            if idx < len(a):
                out.append(f'{pname}={a[idx]}')
                continue
        try:
            # let's check out if fed as positional
            val = a[idx]
            out.append(f'{pname}={val}')
            continue
        except:
            pass
        # we're in keyword land
        if pname in kw:
            val = kw[pname]
        else:
            val = param.default
        out.append(f'{pname}={val}')
    return '-'.join(out)


def _extract_from_expr(expr):
    # from an autotrophic expression, extract
    # the function name, the function object
    # the full args (to match python args) and kwargs
    from tshistory_formula.interpreter import NullIntepreter

    fname = str(expr[0])
    func = FUNCS[fname]
    # because auto operators have an interpreter
    # and __from/__to/__revision_date__
    args = [NullIntepreter(), None, None, None]
    kwargs = {}
    kw = None
    for a in expr[1:]:
        if isinstance(a, Keyword):
            kw = a
            continue
        if isinstance(a, list):
            a = serialize(a)
        if kw:
            kwargs[str(kw)] = a
            kw = None
            continue
        args.append(a)
    return fname, func, args, kwargs


# tzaware check

def tzlabel(status):
    if status is None:
        return 'unknown'
    return 'tzaware' if status else 'tznaive'


def find_tz(self, cn, tree, tzmap, path=()):
    op = tree[0]
    path = path + (op,)
    metas = METAS.get(op)
    if metas:
        for name, metadata in metas(cn, self, tree).items():
            tzaware = metadata['tzaware'] if metadata else None
            if 'naive' in path:
                tzaware = False
            tzmap[(name, path)] = tzaware

    if op == 'findseries':
        _, kw = buildargs(tree)
        tzmap[(serialize(tree[1]), path)] = not kw.get('naive', False)

    for item in tree:
        if isinstance(item, list):
            find_tz(self, cn, item, tzmap, path)


# thread pool

class _WorkItem(object):
    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException as exc:
            self.future.set_exception(exc)
        else:
            self.future.set_result(result)


class Stop:
    pass


class ThreadPoolExecutor:

    def __init__(self, max_workers):
        self._max_workers = max_workers
        self._work_queue = queue.SimpleQueue()
        self._threads = set()
        self._shutdown = False
        self._shutdown_lock = threading.Lock()

    def _worker(self):
        while True:
            work_item = self._work_queue.get(block=True)
            if work_item is Stop:
                # allow the other workers to get it
                self._work_queue.put(Stop)
                return
            work_item.run()

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            f = _base.Future()

            self._work_queue.put(_WorkItem(f, fn, args, kwargs))
            num_threads = len(self._threads)
            if num_threads < self._max_workers:
                t = threading.Thread(target=self._worker)
                t.start()
                self._threads.add(t)
            return f

    def shutdown(self):
        with self._shutdown_lock:
            self._shutdown = True
            self._work_queue.put(Stop)
            for t in self._threads:
                t.join()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        return False


def rewrite_trig_formula(tree):
    if not isinstance(tree, list):
        return tree

    op = tree[0]
    if op in ('trig.tan', 'trig.cos', 'trig.sin'):
        posargs, kwargs = buildargs(tree[1:])
        if 'decimals' in posargs:
            decimals = posargs['decimals']
            tree = tree[:-1]
        elif 'decimals' in kwargs:
            decimals = kwargs['decimals']
            tree = tree[:-2]
        else:
            return tree
        newtree = [
            Symbol('round'), tree, Keyword('decimals'), decimals
        ]
        return newtree

    return [
        rewrite_trig_formula(item)
        for item in tree
    ]


def rewrite_sub_formula(tree):
    if not isinstance(tree, list):
        return tree

    op = tree[0]
    if op == 'add':
        posargs, _kwargs = buildargs(tree[1:])
        positive_elements = []
        negative_elements = []
        for element in posargs:
            if element[0] == '*' and element[1] == -1:
                negative_elements.append(element[2])
            else:
                positive_elements.append(element)

        if len(negative_elements) == 0:
            return tree

        if len(positive_elements) > 1:
            positive_tree = [Symbol('add')]
            positive_tree.extend(positive_elements)
        else:
            positive_tree = positive_elements[0]

        if len(negative_elements) > 1:
            negative_tree = [Symbol('add')]
            negative_tree.extend(negative_elements)
        else:
            negative_tree = negative_elements[0]

        newtree = [
            Symbol('sub'), positive_tree, negative_tree
        ]
        return newtree

    return [
        rewrite_sub_formula(item)
        for item in tree
    ]
