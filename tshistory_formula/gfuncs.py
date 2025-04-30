import uuid
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
def group_add(*grouplist: pd.DataFrame) -> pd.DataFrame:
    """
    Linear combination of two or more groups. Takes a variable number
    of groups as input.

    Example: `(group-add (group "wallonie") (group "bruxelles") (group "flandres"))`

    """
    if not grouplist:
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

    sumdf = sum(grouplist)

    return sumdf.dropna()


@gfunc('group-add-series')
def group_add_series(group: pd.DataFrame,
                     series: pd.Series) -> pd.DataFrame:
    """
    Linear combination of a group and a series. Takes one group
    and one series as input. The series will be added to each scenario of the group.

    Example: `(group-add-series (group "belgium_temperature_kelvin") (series "kelvin_to_degree"))`

    """

    if not len(group):
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

    fillopt = series.options
    if 'fill' in fillopt:
        if fillopt['fill'] is not None:
            seriesname = str(uuid.uuid4())
            df = pd.concat([group, series.to_frame(seriesname)], axis=1, join='outer')
            _fill(df, seriesname, fillopt)
            return group.add(df[seriesname], axis=0).dropna()

    return group.add(series, axis=0).dropna()


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
