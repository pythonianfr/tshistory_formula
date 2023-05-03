import json
import inspect
from functools import partial
from datetime import datetime

import pytz
import pandas as pd
from psyl.lisp import (
    Env,
    parse
)

from tshistory.util import empty_series
from tshistory_formula.evaluator import pevaluate

from tshistory_formula import (
    helper,
    types,
    registry
)


def functypes(all=False):
    return {
        name: types.function_types(func)
        for name, func in registry.FUNCS.items()
        if all or func.__doc__ is not None
    }


def jsontypes(all=False):
    return json.dumps(functypes(all=all))


class Interpreter:
    __slots__ = ('env', 'cn', 'tsh', 'getargs', 'histories', 'vcache', 'auto')
    FUNCS = None

    @property
    def operators(self):
        if Interpreter.FUNCS is None:
            Interpreter.FUNCS = registry.FUNCS
        return Interpreter.FUNCS

    def __init__(self, cn, tsh, getargs):
        self.cn = cn
        self.tsh = tsh
        self.getargs = getargs
        # bind funcs to the interpreter
        funcs = {}
        for name, func in self.operators.items():
            if '__interpreter__' in inspect.getfullargspec(func).args:
                func = partial(func, self)
            funcs[name] = func
        funcs['#t'] = True
        funcs['#f'] = False
        funcs['nil'] = None
        self.env = Env(funcs)
        self.histories = {}
        self.vcache = {}
        self.auto = set(registry.AUTO.values())

    def get(self, name, getargs):
        # `getarg` likey comes from self.getargs
        # but we allow it being modified hence
        # it comes back as a parameter there
        return self.tsh.get(self.cn, name, **getargs)

    def evaluate(self, tree):
        return pevaluate(tree, self.env, self.auto, self.tsh.concurrency)

    def today(self, naive, tz):
        if naive:
            assert tz is None, 'date cannot be naive and have a tz'
        tz = pytz.timezone(tz or 'utc')
        key = ('today', naive, tz)

        val = self.getargs.get('revision_date')
        if val:
            # we don't use the cache because revision_date may change
            # during this interpreter life time (e.g. history calls)
            if naive:
                val = val.replace(tzinfo=None)
            elif val.tzinfo is None:
                val = pd.Timestamp(val, tz=tz)
            else:
                val = val.tz_convert(tz)
            self.vcache[key] = val
            return val

        val = self.vcache.get(key)
        if val is not None:
            return val

        if naive:
            val = pd.Timestamp(datetime.today())
        else:
            val = pd.Timestamp(datetime.today(), tz=tz)

        self.vcache[key] = val
        return val


class OperatorHistory(Interpreter):
    __slots__ = ('env', 'cn', 'tsh', 'getargs')
    FUNCS = None

    @property
    def operators(self):
        if OperatorHistory.FUNCS is None:
            OperatorHistory.FUNCS = {**registry.FUNCS, **registry.HISTORY}
        return OperatorHistory.FUNCS

    def evaluate_history(self, tree):
        return pevaluate(
            tree,
            self.env,
            self.auto,
            self.tsh.concurrency,
            hist=True
        )


class HistoryInterpreter(Interpreter):
    __slots__ = 'env', 'cn', 'tsh', 'getargs', 'histories', 'tzaware', 'namecache', 'vcache'

    def __init__(self, name, *args, histories):
        super().__init__(*args)
        self.histories = histories
        # a callsite -> name mapping
        self.namecache = {}
        self.tzaware = self.tsh.internal_metadata(self.cn, name)['tzaware']

    def _find_by_nearest_idate(self, name, idate):
        hist = self.histories[name]
        tzaware = idate.tzinfo is not None
        for date in reversed(list(hist.keys())):
            compdate = date
            if not tzaware:
                compdate = date.replace(tzinfo=None)
            if idate >= compdate:
                return hist[date]

        ts = empty_series(
            self.tzaware,
            name=name
        )
        return ts

    def get(self, name, _getargs):
        # getargs is moot there because histories
        # have been precomputed
        # get the nearest inferior or equal for the given
        # insertion date
        assert self.histories
        return self._find_by_nearest_idate(
            name,
            self.getargs['revision_date']
        )

    def get_auto(self, tree):
        """ helper for autotrophic series that have pre built their
        history and are asked for one element
        (necessary since they bypass the above .get)
        """
        name = self.namecache.get(id(tree))
        if name is None:
            name = helper.name_of_expr(tree)
            self.namecache[id(tree)] = name
        idate = self.env.get('__idate__')
        assert idate
        return self._find_by_nearest_idate(name, idate)

    def evaluate(self, tree, idate, name):
        # provide ammo to .today
        self.getargs['revision_date'] = idate
        self.env['__name__'] = name
        self.env['__idate__'] = idate
        ts = pevaluate(tree, self.env, self.auto, self.tsh.concurrency, hist=True)
        ts.name = name
        return ts


# staircase fast path


def has_compatible_operators(cn, tsh, tree, good_operators):
    operators = [tree[0]]
    for param in tree[1:]:
        if isinstance(param, list):
            operators.append(param[0])
    if any(op not in good_operators
           for op in operators):
        return False

    names = tsh.find_series(cn, tree)
    for name in names:
        formula = tsh.formula(cn, name)
        if formula:
            tree = parse(formula)
            if not has_compatible_operators(
                    cn, tsh, tree, good_operators):
                return False

    return True


class FastStaircaseInterpreter(Interpreter):
    __slots__ = ('env', 'cn', 'tsh', 'getargs', 'delta')

    def __init__(self, cn, tsh, getargs, delta):
        assert delta is not None
        super().__init__(cn, tsh, getargs)
        self.delta = delta

    def get(self, name, getargs):
        if self.tsh.type(self.cn, name) == 'primary':
            # true enough, .staircase does not handle revision_date
            getargs.pop('revision_date')
            return self.tsh.staircase(
                self.cn, name, delta=self.delta, **getargs
            )
        return self.tsh.get(
            self.cn, name, **getargs,
            __interpreter__=self
        )


class NullIntepreter(Interpreter):

    def __init__(self):
        pass


# groups


class GroupInterpreter(Interpreter):
    __slots__ = 'env', 'cn', 'tsh', 'getargs', 'histories', 'vcache', 'auto'
    FUNCS = None

    @property
    def operators(self):
        if GroupInterpreter.FUNCS is None:
            GroupInterpreter.FUNCS = dict(registry.FUNCS, **registry.GFUNCS)
        return GroupInterpreter.FUNCS


class BridgeInterpreter(Interpreter):
    """Intepreter that creates a bridge between the group world and the
    series world
    """
    __slots__ = ('env', 'cn', 'tsh', 'getargs', 'histories', 'vcache', 'auto',
                 'groups', 'binding', 'memory_cache')

    def __init__(self, *args, groups, binding):
        super().__init__(*args)
        self.groups = groups
        self.binding = binding
        self.memory_cache = {}

    def get(self, seriesname, _getargs):
        bound_series = self.binding['series'] == seriesname
        seriescount = sum(bound_series)
        if seriescount == 0:
            key = (seriesname, *list(_getargs.values()))
            if key not in self.memory_cache:
                result = super().get(seriesname, _getargs)
                self.memory_cache[key] = result
                return result
            return self.memory_cache[key]
        elif seriescount > 1:
            raise Exception

        family = self.binding.loc[bound_series, 'family'].iloc[0]
        combination = self.env['__combination__']
        return self.groups[family][seriesname][combination[family]]

    def g_evaluate(self, text, combination):
        self.env['__combination__'] = combination
        ts = pevaluate(parse(text), self.env, (), self.tsh.concurrency)
        ts.name = '.'.join([
            str(sn)
            for _, sn, in combination.items()
        ])
        return ts
