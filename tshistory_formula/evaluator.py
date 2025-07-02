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
from tshistory_formula.registry import FUNC_METADATA, QARGS


NONETYPE = type(None)


@cache
def funcid(func):
    return hash(inspect.getsource(func))


# parallel evaluator

def resolve(atom, env):
    if isinstance(atom, Symbol):
        return env.find(atom)
    assert isinstance(atom, (int, float, str, NONETYPE))
    return atom


def _evaluate(tree, env, funcids=(), pool=None):
    if not isinstance(tree, list):
        # we've got an atom
        # we do this very late rather than upfront
        # because the interpreter will need the original
        # symbolic expression to build names
        return resolve(tree, env)

    if tree[0] == 'let':
        newtree, newenv = let(
            env, tree[1:],
            partial(_evaluate, funcids=funcids, pool=pool)
        )
        # the env grows new bindigs
        # the tree has lost its let-definition
        return _evaluate(newtree, newenv, funcids, pool)

    # a functional expression
    # the recursive evaluation will
    # * dereference the symbols -> functions
    # * evaluate the sub-expressions -> values
    exps = [
        _evaluate(exp, env, funcids, pool)
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

    # get function name from the expression tree for metadata lookup
    func_name = str(tree[0])

    # use pre-computed metadata instead of runtime signature inspection
    metadata = FUNC_METADATA.get(func_name, {})

    # handle varargs using pre-computed metadata
    if metadata.get('has_varargs', False):
        if len(posargs) == 1 and isinstance(posargs[0], list):
            posargs = posargs[0]

    # inject environment arguments using pre-computed list
    injectable_args = metadata.get('injectable_args', [])
    if injectable_args:
        injected = [env.find(QARGS[arg]) for arg in injectable_args]
        posargs = injected + posargs

    # for async execution, we still need to identify the underlying function
    # open partials to find the true operator on which we can decide
    # to go async
    if hasattr(proc, 'func'):
        func = proc.func
    else:
        func = proc

    # an async function, e.g. series, being I/O oriented
    # can be deferred to a thread
    funkey = funcid(func)
    if funkey in funcids and pool:
        return pool.submit(proc, *posargs, **kwargs)

    # at this point, we have a function, and all the arguments
    # have been evaluated, so we do the final call
    return proc(*posargs, **kwargs)


def pevaluate(tree, env, asyncfuncs=(), concurrency=16):
    if concurrency > 1:
        with ThreadPoolExecutor(concurrency) as pool:
            val = _evaluate(
                tree,
                env,
                {funcid(func) for func in asyncfuncs},
                pool
            )
            if isinstance(val, Future):
                val = val.result()
        return val

    return _evaluate(
        tree,
        env,
        {funcid(func) for func in asyncfuncs},
        None
    )
