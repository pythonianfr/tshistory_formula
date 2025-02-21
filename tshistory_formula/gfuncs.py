from typing import Union

import pandas as pd

from tshistory_formula.registry import (
    gfunc,
    gfinder,
)


@gfunc('group')
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
    tss = [
        ts for ts in grouplist
        if isinstance(ts, pd.Series)
    ]

    if not len(dfs):
        raise Exception('group-add: at least one argument must be a group')

    sumdf = sum(dfs)
    sumts = sum(tss)

    return sumdf.add(sumts, axis=0).dropna()


@gfinder('group')
def group_finder(cn, tsh, stree):
    name = stree[1]
    return {name: stree}
