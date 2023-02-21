import inspect
from concurrent.futures import (
    Future
)
from functools import partial

try:
    from functools import cache
except ImportError:
    # before python 3.9
    _CACHE = {}
    def cache(func):
        def wrapper(*a, **k):
            val = _CACHE.get(a)
            if val:
                return val
            _CACHE[a] = val = func(*a, **k)
            return val
        return wrapper


from psyl.lisp import (
    buildargs,
    let,
    Symbol
)

from tshistory_formula.helper import ThreadPoolExecutor


NONETYPE = type(None)


@cache
def funcid(func):
    return hash(inspect.getsource(func))


QARGS = {
    '__from_value_date__': 'from_value_date',
    '__to_value_date__': 'to_value_date',
    '__revision_date__': 'revision_date'
}


# parallel evaluator

def resolve(atom, env):
    if isinstance(atom, Symbol):
        return env.find(atom)
    assert isinstance(atom, (int, float, str, NONETYPE))
    return atom


def _evaluate(tree, env, funcids=(), pool=None, hist=False):
    if not isinstance(tree, list):
        # we've got an atom
        # we do this very late rather than upfront
        # because the interpreter will need the original
        # symbolic expression to build names
        return resolve(tree, env)

    if tree[0] == 'let':
        newtree, newenv = let(
            env, tree[1:],
            partial(_evaluate, funcids=funcids, pool=pool, hist=hist)
        )
        # the env grows new bindigs
        # the tree has lost its let-definition
        return _evaluate(newtree, newenv, funcids, pool, hist)

    # a functional expression
    # the recursive evaluation will
    # * dereference the symbols -> functions
    # * evaluate the sub-expressions -> values
    exps = [
        _evaluate(exp, env, funcids, pool, hist)
        for exp in tree
    ]
    # since some calls are evaluated asynchronously (e.g. series) we
    # need to resolve all the future objects
    newargs = [
        arg.result() if isinstance(arg, Future) else arg
        for arg in exps[1:]
    ]
    proc = exps[0]
    posargs, kwargs = buildargs(newargs)

    # open partials to find the true operator on which we can decide
    # to go async
    if hasattr(proc, 'func'):
        func = proc.func
    else:
        func = proc

    # for autotrophic operators: prepare to pass the tree if present
    funkey = funcid(func)
    if hist and funkey in funcids:
        kwargs['__tree__'] = tree

    signature = inspect.getfullargspec(func)
    if signature.varargs:
        if len(posargs) == 1 and isinstance(posargs[0], list):
            posargs = posargs[0]
    # prepare args injection from the lisp environment
    posargs = [
        env.find(QARGS[arg]) for arg in signature.args
        if arg in QARGS
    ] + posargs


    # an async function, e.g. series, being I/O oriented
    # can be deferred to a thread
    if funkey in funcids and pool:
        return pool.submit(proc, *posargs, **kwargs)

    # at this point, we have a function, and all the arguments
    # have been evaluated, so we do the final call
    return proc(*posargs, **kwargs)


def pevaluate(tree, env, asyncfuncs=(), concurrency=16, hist=False):
    if concurrency > 1:
        with ThreadPoolExecutor(concurrency) as pool:
            val = _evaluate(
                tree,
                env,
                {funcid(func) for func in asyncfuncs},
                pool,
                hist
            )
            if isinstance(val, Future):
                val = val.result()
        return val

    return _evaluate(
        tree,
        env,
        {funcid(func) for func in asyncfuncs},
        None,
        hist
    )
