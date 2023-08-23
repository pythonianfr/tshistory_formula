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
    Symbol,
    Keyword,
    Env,
    serialize,
    parse,
    pairwise
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


# cached implementation

import msgpack
from functools import cache
import pandas as pd
from typing import List, AnyStr


def cache_notify(func):
    func = cache(func)

    def notify_wrapper(*args, **kwargs):
        stats = func.cache_info()
        hits = stats.hits
        results = func(*args, **kwargs)
        stats = func.cache_info()
        if stats.hits > hits:
            print(f"CACHE: {args[0]} results were cached {stats.hits} hits")
        print(stats)
        return results

    return notify_wrapper


def __let_hack(env, bindings_expr, evaluator):
    newenv = Env()
    bindings = bindings_expr[:-1]
    tree = bindings_expr[-1]
    for sym, val in pairwise(bindings):
        serialized_tree = serialize_tree(val)
        serialized_qargs = serialize_qargs(env)
        res = evaluator(
            serialized_tree,
            serialized_qargs,
            type(serialized_tree).__name__,
        )
        newenv[sym] = res
    # better to have no outer for the serialisation process.
    # Concatenation already creates the priority (left priority)
    newenv = Env({**env, **newenv})
    return tree, newenv


def let_hack(env, bindings_expr_str, evaluator):
    return __let_hack(env, parse(bindings_expr_str), evaluator)


def serialize_qargs(env_add: Env) -> str:
    env = {
        k: v.isoformat(timespec='milliseconds')
        if isinstance(v, pd.Timestamp) else v
        for k, v in env_add.items()
    }
    return msgpack.packb(env)


def deserialize_qargs(packed_env: str) -> Env:
    env = msgpack.unpackb(packed_env)
    _env = dict()
    for k, v in env.items():
        if v is not None:
            _env[k] = pd.to_datetime(v)
        else:
            _env[k] = v
    return Env(_env)


def serialize_tree(tree: AnyStr or List):
    return serialize(tree) if isinstance(tree, list) else tree


def deserialize_tree(stringed_tree: str):
    is_lisp_atom = type(stringed_tree) in (Keyword, Symbol)
    # Now need to check if its a formula or keyword
    not_a_formula = not (stringed_tree.startswith('(') or stringed_tree.startswith('#'))
    return stringed_tree if (is_lisp_atom or not_a_formula) else parse(stringed_tree)


def cache_pevaluate(tree, env, asyncfuncs=(), concurrency=16, hist=False):
    pool = None
    funcids = {funcid(func) for func in asyncfuncs}

    @cache
    def __evaluate(
            serialized_tree: str,
            serialized_env: str,
            tree_type: str,
            hist: bool = False
    ):
        a_string = tree_type in ('str', 'Keyword', 'Symbol')
        if not a_string:  # int or None
            return serialized_tree
        # Unpacking the tree and the env from strings to their respective structures
        tree = deserialize_tree(serialized_tree)
        query_args = deserialize_qargs(serialized_env)
        # Merging the query parameters with the wider environment
        local_env = Env({**env, **query_args})
        if not isinstance(tree, list):
            # we've got an atom
            # we do this very late rather than upfront
            # because the interpreter will need the original
            # symbolic expression to build names
            return resolve(tree, local_env)

        if tree[0] == 'let':
            newtree, newenv = let_hack(
                query_args, serialize_tree(tree[1:]),
                partial(__evaluate, hist=hist)
            )
            # the env grows new bindigs
            # the tree has lost its let-definition
            serialized = serialize_tree(newtree)
            return __evaluate(
                serialized,
                serialize_qargs(newenv),
                type(serialized).__name__,
                hist
            )

        # a functional expression
        # the recursive evaluation will
        # * dereference the symbols -> functions
        # * evaluate the sub-expressions -> values
        exps = [
            __evaluate(
                serialize_tree(exp),
                serialize_qargs(query_args),
                type(serialize_tree(exp)).__name__,
                hist
            )
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
                      local_env.find(QARGS[arg]) for arg in signature.args
                      if arg in QARGS
                  ] + posargs

        # an async function, e.g. series, being I/O oriented
        # can be deferred to a thread
        if funkey in funcids and pool:
            return pool.submit(proc, *posargs, **kwargs)

        # at this point, we have a function, and all the arguments
        # have been evaluated, so we do the final call
        return proc(*posargs, **kwargs)

    if concurrency > 1:
        with ThreadPoolExecutor(concurrency) as pool:
            val = __evaluate(
                serialize(tree),
                serialize_qargs(Env({})),
                type(serialize(tree)).__name__,
                hist
            )
            if isinstance(val, Future):
                val = val.result()
        return val

    return __evaluate(
        serialize(tree),
        serialize_qargs(Env({})),
        type(serialize(tree)).__name__,
        hist
    )
