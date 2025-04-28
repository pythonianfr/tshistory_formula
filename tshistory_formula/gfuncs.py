from typing import Union

import pandas as pd

from tshistory_formula.funcs import _fill
from tshistory_formula.types import Bind
from tshistory_formula.registry import (
    gfunc,
    gfinder,
)


@gfunc('group', auto=True)
def group(__interpreter__, name: str)-> pd.DataFrame:
    """
    The `group` operator retrieves a group (from local storage,
    formula or bound formula).

    """
    i = __interpreter__
    exists = i.tsh.group_exists(i.cn, name)
    if not exists:
        if i.tsh.othersources and i.tsh.othersources.group_exists(name):
            return i.tsh.othersources.group_get(name, **i.getargs)

    if not exists:
        raise ValueError(f'No such group `{name}`')

    return i.tsh.group_get(
        i.cn, name,  **i.getargs
    )


@gfunc('group-add')
def group_add(*grouplist: Union[pd.DataFrame, pd.Series]) -> pd.DataFrame:
    """
    Linear combination of two or more groups. Takes a variable number
    of groups and series as input. At least one group must be supplied.

    Example: `(group-add (group "wallonie") (group "bruxelles") (group "flandres"))`

    """
    dfs = [
        df for df in grouplist
        if isinstance(df, pd.DataFrame)
    ]

    if not len(dfs):
        # by default, yield an empty timezone aware group
        empty_df = pd.DataFrame(
            [],
            index=pd.DatetimeIndex(
                [],
                tz='UTC'
            ),
            dtype='float64',
            columns=None
        )
        return empty_df

    tss = [
        ts for ts in grouplist
        if isinstance(ts, pd.Series)
    ]

    sumdf = sum(dfs)
    sumts = sum(tss)

    return sumdf.add(sumts, axis=0).dropna()


@gfinder('group')
def group_finder(cn, tsh, stree):
    name = stree[1]
    return {name: stree}


@gfunc('bind')
def bind(scenario_name: str, series: pd.Series) -> Bind:
    """
    Build a bind from a scenario name and a series.
    """
    return Bind(scenario_name, series)


@gfunc('group-from-series')
def group_from_series(*bindlist: Bind) -> pd.DataFrame:
    """
    Group construction from a list of bind.

    Example: `(group-from-series (bind "scenario1" (series "series1")) (bind "scenario2" (series "series2")) (bind "scenario3" (series "series3")))`

    """
    dfs = []
    opts = {}

    # join everything
    for member in bindlist:
        ts = member.get_ts()
        if ts.options.get('fill') is None and not len(ts):
            # no data and no fill
            continue

        opts[ts.name] = ts.options
        dfs.append(ts)

    if not len(dfs):
        # by default, yield an empty timezone aware group
        empty_df = pd.DataFrame(
            [],
            index=pd.DatetimeIndex(
                [],
                tz='UTC'
            ),
            dtype='float64',
            columns=None
        )
        return empty_df

    df = pd.concat(dfs, axis=1, join='outer')

    # apply the filling rules to series
    for name, fillopt in opts.items():
        if fillopt:
            _fill(df, name, fillopt)

    return df
