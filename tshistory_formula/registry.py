import inspect
from warnings import warn

import pandas as pd

from tshistory_formula.decorator import decorate


FUNCS = {}
IDATES = {}
METAS = {}
FINDERS = {}
AUTO = {}
ARGSCOPES = {}


def _ensure_options(obj):
    if isinstance(obj, pd.Series):
        if not getattr(obj, 'options', None):
            obj.options = {}
    return obj


def func(name, auto=False):
    # work around the circular import
    from tshistory_formula.types import assert_typed

    def decorator(func):
        assert_typed(func)

        def operator(func, *a, **kw):
            res = func(*a, **kw)
            return _ensure_options(
                res
            )

        dec = decorate(func, operator)

        FUNCS[name] = dec
        if auto:
            AUTO[name] = func

            for posarg in ('__interpreter__',
                           '__from_value_date__',
                           '__to_value_date__',
                           '__revision_date__'):
                assert posarg in inspect.getfullargspec(func).args, (
                    f'`{name}` is an autotrophic operator. '
                    f'It should have a `{posarg}` positional argument.'
                )

        return dec

    return decorator


def history(name):

    def decorator(func):
        raise ValueError(
            'The @history decorator is gone. You should use @insertion_dates'
        )

    return decorator


def insertion_dates(name):

    def decorator(func):
        assert name in AUTO, f'operator {name} is not declared as "auto"'
        IDATES[name] = func
        return func

    return decorator



_KEYS = set([
    'index_dtype',
    'index_type',
    'tzaware',
    'value_dtype',
    'value_type'
])


def metadata(name):

    def decorator(func):
        def _ensure_meta_keys(func, *a, **kw):
            res = func(*a, **kw)
            for name, meta in res.items():
                if meta is None:
                    # underlying series is void, must be
                    # register_formula(..., reject_unknown=False)
                    continue
                missing = _KEYS - set(meta.keys())
                if len(missing):
                    warn(
                        f'{name} has missing metadata keys ({missing})'
                    )
            return res

        dec = decorate(func, _ensure_meta_keys)
        METAS[name] = dec
        return dec

    return decorator


def finder(name):

    def decorator(func):
        FINDERS[name] = func
        return func

    return decorator


def argscope(name, params):

    def decorator(func):
        ARGSCOPES[name] = params
        return func

    return decorator


# groups

GFUNCS = {}
GFINDERS = {}
GMETAS = {}
GHISTORY = {}
GAUTO = {}
GIDATES = {}


def gfunc(name, auto=False):
    # work around the circular import
    from tshistory_formula.types import assert_typed

    def decorator(func):
        assert_typed(func)
        GFUNCS[name] = func
        return func

    if auto:
        GAUTO[name] = func

    return decorator


def gfinder(name):

    def decorator(func):
        GFINDERS[name] = func
        return func

    return decorator


def gmeta(name):

    def decorator(func):
        def _ensure_meta_keys(func, *a, **kw):
            res = func(*a, **kw)
            for name, meta in res.items():
                missing = _KEYS - set(meta.keys())
                if len(missing):
                    warn(
                        f'{name} has missing metadata keys ({missing})'
                    )
            return res

        dec = decorate(func, _ensure_meta_keys)
        GMETAS[name] = dec
        return dec

    return decorator


def ginsertion_dates(name):

    def decorator(func):
        assert name in GAUTO, f'operator {name} is not declared as "auto"'
        GIDATES[name] = func
        return func

    return decorator
