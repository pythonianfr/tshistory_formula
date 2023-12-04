import numpy as np
import pandas as pd
from datetime import datetime as dt
from datetime import date
from numpy.random import randn
from psyl.lisp import parse
from tshistory_formula.interpreter import Interpreter
from tshistory.testutil import (
    assert_df,
    assert_hist,
    gengroup,
    utcdt
)

from tshistory_formula.registry import (
    func
)

import time
import pytest


def test_cache(engine, tsh):
    series = pd.Series(
        np.sin(np.linspace(0, 300, 300)),
        index=pd.date_range(dt(2019, 1, 1), periods=300, freq='D')
    ).round(0)
    tsh.update(engine, series, 'cacher', 'Babar',
               insertion_date=utcdt(2019, 1, 1))
    very_cache_friendly_formula = '''
    (add 
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
        (cumsum (cumsum (series "cacher")))
    )
    '''
    tsh.register_formula(
        engine,
        'nested_cumsum',
        very_cache_friendly_formula
    )
    args = {
        'revision_date': None,
        'from_value_date': dt(2019, 4, 1),
        'to_value_date': dt(2019, 4, 7),
    }
    interpreter = Interpreter(
        engine,
        tsh,
        getargs=args
    )
    tree = tsh._expanded_formula(
        engine,
        very_cache_friendly_formula,
        qargs=args
    )
    cache_start = time.time()
    res_cache = interpreter.evaluate(tree, cache=True)
    cache_end = time.time()
    res = interpreter.evaluate(tree, cache=False)
    uncached_end = time.time()

    assert_df(
        """
2019-04-01    10.0
2019-04-02    20.0
2019-04-03    20.0
2019-04-04    10.0
2019-04-05     0.0
2019-04-06     0.0
2019-04-07    10.0
""", res_cache)

    assert_df(
        """
2019-04-01    10.0
2019-04-02    20.0
2019-04-03    20.0
2019-04-04    10.0
2019-04-05     0.0
2019-04-06     0.0
2019-04-07    10.0
""", res)

    # Validate speedups

    cached_time = cache_end - cache_start
    uncached_time = uncached_end - cache_end
    speedup = 100 * ((uncached_time / cached_time) - 1)
    assert (
        speedup > 100  # %
        if tsh.concurrency == 16
        else speedup > 50  # %
    ), print(
        f"""Speedup achieved {speedup:.0f} %, 
        below our desired speedup for {tsh.concurrency} threads
        """)


def test_multiget(engine, tsh):
    series = pd.Series(
        np.sin(np.linspace(0, 300, 300)),
        index=pd.date_range(dt(2019, 1, 1), periods=300, freq='D')
    ).round(0)
    series_names = [
        'cacher',
        'cumsum_cacher',
        'cacher_squared',
        'cacher_cubed',
    ]
    tsh.update(engine, series, 'cacher', 'Babar',
               insertion_date=utcdt(2019, 1, 1))

    @func('very_big_formula')
    def very_big_formula(series: pd.Series) -> pd.Series:
        time.sleep(1)  # seconds
        return series

    tsh.register_formula(
        engine,
        'cumsum_cacher',
        '(very_big_formula (series "cacher"))'
    )
    tsh.register_formula(
        engine,
        'cacher_squared',
        '(** (very_big_formula (series "cacher")) 2)'
    )
    tsh.register_formula(
        engine,
        'cacher_cubed',
        '(** (very_big_formula (series "cacher")) 3)'
    )
    args = {
        'revision_date': None,
        'from_value_date': dt(2019, 4, 1),
        'to_value_date': dt(2019, 4, 7),
    }

    def naive_multiget(engine, tsh, series_names, **kw):
        _df = pd.concat([tsh.get(engine, s, **kw) for s in series_names], axis=1)
        return pd.DataFrame(
            _df.values,
            index=_df.index,
            columns=series_names
        )

    def multiget(engine, tsh, series_names, **kw):
        # Very hacky implementation idea for now, not even integrated to tsio.py out of respect.
        # It does not even handle :#fill args yet.. i quite like the wide format in the end, which could be used by _xl
        if all(map(lambda name: tsh.exists(engine, name), series_names)):
            group_series = " ".join(["(group_series", *[f'(series "{s}")' for s in series_names], ")"])
            interpreter = Interpreter(
                engine,
                tsh,
                getargs=kw
            )

            tree = tsh._expanded_formula(
                engine,
                group_series,
                qargs=kw
            )
            _df = interpreter.evaluate(tree, cache=True)
            return pd.DataFrame(
                _df.values,
                index=_df.index,
                columns=series_names
            )
        else:
            raise IOError(f'All series names must exist')

    cache_start = time.time()
    df = multiget(engine, tsh, series_names, **args)
    cache_end = time.time()
    naive_df = naive_multiget(engine, tsh, series_names, **args)
    uncached_end = time.time()
    assert_df(
        """
            cacher  cumsum_cacher  cacher_squared  cacher_cubed
2019-04-01     1.0            1.0             1.0           1.0
2019-04-02    -0.0           -0.0             0.0          -0.0
2019-04-03    -1.0           -1.0             1.0          -1.0
2019-04-04    -1.0           -1.0             1.0          -1.0
2019-04-05     0.0            0.0             0.0           0.0
2019-04-06     1.0            1.0             1.0           1.0
2019-04-07     1.0            1.0             1.0           1.0
        """, df)

    assert_df(
        """
            cacher  cumsum_cacher  cacher_squared  cacher_cubed
2019-04-01     1.0            1.0             1.0           1.0
2019-04-02    -0.0           -0.0             0.0          -0.0
2019-04-03    -1.0           -1.0             1.0          -1.0
2019-04-04    -1.0           -1.0             1.0          -1.0
2019-04-05     0.0            0.0             0.0           0.0
2019-04-06     1.0            1.0             1.0           1.0
2019-04-07     1.0            1.0             1.0           1.0
        """, naive_df)

    with pytest.raises(IOError):
        multiget(engine, tsh, series_names + ['bogus'], **args)

    # Validate speedups

    cached_time = cache_end - cache_start
    uncached_time = uncached_end - cache_end
    speedup = 100 * ((uncached_time / cached_time) - 1)
    assert (
        speedup > 150  # %
        if tsh.concurrency == 16
        else speedup > 150  # %
    ), print(
        f"""Speedup achieved {speedup:.0f} %, 
        below our desired speedup for {tsh.concurrency} threads
        """)
