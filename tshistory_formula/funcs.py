from datetime import date, timedelta, datetime
from typing import List, Union, Optional, Tuple
from numbers import Number
import calendar
from functools import reduce
import operator

import holidays
import numpy as np
import pandas as pd
import pycountry
import pytz
from dateutil.relativedelta import relativedelta

from psyl.lisp import (
    buildargs,
    Keyword,
    Symbol,
)
from tshistory.util import (
    compatible_date,
    empty_series,
    ensuretz,
    patch,
    patchmany,
    threadpool,
    tzaware_serie
)
from tshistory import search
from tshistory_formula.registry import (
    finder,
    func,
    history,
    insertion_dates,
    metadata,
    argscope
)
from tshistory_formula.types import NONETYPE
from tshistory_formula.helper import seriesname
from tshistory_formula.interpreter import Interpreter


@func('options')
def options(series: pd.Series,
            fill: Union[str, Number, NONETYPE]=None,
            limit: Optional[int]=None,
            weight: Optional[Number]=None) -> pd.Series:
    """
    The `options` operator takes a series and three keywords to modify
    the behaviour of series.

    * `fill` to specify a filling policy to avoid `nans` when the
      series will be `add`ed with others; accepted values are
      `"ffill"` (forward-fill), `"bfill"` (backward-fill) or any
      floating value.

    * `limit`: if `fill` is specified, this is the maximum number of
      consecutive NaN values to forward/backward fill. In other words,
      if there is a gap with more than this number of consecutive
      NaNs, it will only be partially filled. If `fill` is not
      specified, this is the maximum number of entries along the
      entire axis where NaNs will be filled. Must be greater than 0 if
      not None.

    * `weight` to provide a weight (float) value to be used by other
      operators like e.g. `row-mean`

    The `fill`, `limit` and `weight` options are put on the series object for
    later use.

    """
    series.options = {
        'fill': fill,
        'limit': limit
    }

    if weight is not None:
        series.options['weight'] = weight

    return series


def _normalize_dates(dates):
    if (all(d.tzinfo is None for d in dates) or
        all(d.tzinfo is not None for d in dates)):
        return dates

    for d in dates:
        if d.tzinfo:
            # we know there must be one
            tzone = d.tzinfo.zone
    return [
        d.tz_localize(tzone) if not d.tzinfo else d
        for d in dates
    ]


@func('min')
def scalar_min(*args: Number) -> Number:
    args = list(filter(None, args))
    if args and  isinstance(args[0], datetime):
        args = _normalize_dates(args)
    return min(args)


@func('max')
def scalar_max(*args: Number) -> Number:
    args = list(filter(None, args))
    if args and  isinstance(args[0], datetime):
        args = _normalize_dates(args)
    return max(args)


@func('series', auto=True)
def series(__interpreter__,
           __from_value_date__,
           __to_value_date__,
           __revision_date__,
           name: seriesname,
           fill: Union[str, Number, NONETYPE]=None,
           limit: Optional[int]=None,
           weight: Optional[Number]=None) -> pd.Series:
    """
    Returns a time series by name.

    The `series` operator accepts several keywords:

    * `fill` to specify a filling policy to avoid `nans` when the
      series will be `add`ed with others; accepted values are
      `"ffill"` (forward-fill), `"bfill"` (backward-fill) or any
      floating value.

    * `limit`: if `fill` is specified, this is the maximum number of
      consecutive NaN values to forward/backward fill. In other words,
      if there is a gap with more than this number of consecutive
      NaNs, it will only be partially filled. If `fill` is not
      specified, this is the maximum number of entries along the
      entire axis where NaNs will be filled. Must be greater than 0 if
      not None.

    * `weight` to provide a weight (float) value to be used by other
      operators like e.g. `row-mean`

    For instance in `(add (series "a" #:fill 0) (series "b")` will
    make sure that series `a`, if shorter than series `b` will get
    zeroes instead of nans where `b` provides values.

    """
    i = __interpreter__
    exists = i.tsh.exists(i.cn, name)
    if not exists:
        if i.tsh.othersources and i.tsh.othersources.exists(name):
            exists = True

    if not exists:
        raise ValueError(f'No such series `{name}`')

    meta = i.tsh.internal_metadata(i.cn, name)
    if meta is None:
        meta = i.tsh.othersources.internal_metadata(name)
    tzaware = meta['tzaware']

    args = {
        'from_value_date': __from_value_date__,
        'to_value_date': __to_value_date__,
        'revision_date': __revision_date__
    }

    ts = i.get(name, args)
    # NOTE: we are cutting there now, but we shouldn't have to
    # the issue lies in the "optimized" way histories are computed:
    # the history interpreter needs a cut there
    ts = ts.loc[
        compatible_date(tzaware, __from_value_date__):
        compatible_date(tzaware, __to_value_date__)
    ]
    ts.options = {
        'fill': fill,
        'limit': limit
    }
    if weight is not None:
        ts.options['weight'] = weight

    return ts


@metadata('series')
def series_metas(cn, tsh, stree):
    name = stree[1]
    meta = tsh.internal_metadata(cn, name)
    # alt sources lookup
    if meta is None and tsh.othersources:
        meta = tsh.othersources.internal_metadata(name)
    return {name: meta}


@finder('series')
def series_finder(cn, tsh, stree):
    name = stree[1]
    return {name: stree}


def serieslist(__interpreter__,
               __from_value_date__,
               __to_value_date__,
               __revision_date__,
               names: List[str],
               fill: Union[str, Number, NONETYPE]=None) -> List[pd.Series]:
    poolrun = threadpool(16)

    result = []
    def collect(name, interpreter, fvd, tvd, rd):
        result.append(
            series(interpreter, fvd, tvd, rd, name, fill=fill)
        )

    poolrun(
        collect,
        [
            (
                name,
                __interpreter__,
                __from_value_date__,
                __to_value_date__,
                __revision_date__
            )
            for name in names
        ]
    )

    return result


@func('findnames')
def findnames(__interpreter__,
              q: search.query) -> List[str]:
    i = __interpreter__
    return i.tsh.find(i.cn, q)


@func('findseries')
def findseries(__interpreter__,
               __from_value_date__,
               __to_value_date__,
               __revision_date__,
               q: search.query,
               naive: bool=False,
               fill: Union[str, Number, NONETYPE]=None) -> List[pd.Series]:
    """
    Yields a series list out of a metadata/name/... filtering query.

    Examples: `(add (findseries (by.value "weight" "<" 43)))`

    The `findseries` operator accepts two keywords:

    * `naive` (defaults to #f) to filter on tzaware or naive series
      (we don't want to mix those there; this is an important
      difference with the `find` API point).

    * `fill` to specify a filling policy to avoid `nans` when the
      series will be `add`ed with others; accepted values are
      `"ffill"` (forward-fill), `"bfill"` (backward-fill) or any
      floating value.

    """
    i = __interpreter__
    if naive:
        q = search.and_(
            search.not_(
                search.tzaware()
            ),
            q
        )
    else:
        q = search.and_(
            search.tzaware(),
            q
        )
    return serieslist(
        __interpreter__,
        __from_value_date__,
        __to_value_date__,
        __revision_date__,
        i.tsh.find(i.cn, q),
        fill
    )


@func('by.name')
def byname(namequery: str) -> search.query:
    """Yields a query filter operating on series names.

    Example: `(add (findseries (by.name "fr capacity")))`

    This will filter the series whose names contain, in order, the
    "fr" and "capacity" fragments.

    """
    return search.byname(namequery)


@func('by.basket')
def bybasket(__interpreter__,
             __from_value_date__,
             __to_value_date__,
             __revision_date__,
             basketname: str) -> search.query:
    """Yields a query filter operating on series names.

    Example: `(add (findseries (by.basket "fr.powerplants")))`

    This will yield the series matching the basket definition.

    """
    i = __interpreter__
    query = i.tsh.basket_definition(i.cn, basketname)
    return search.query.fromexpr(query)


@func('by.metakey')
def bymetakey(keyquery: str) -> search.query:
    """Yields a query filter operating on metadata key.

    Example: `(add (findseries (by.metakey "plant_status")))`

    This will filter the series having "plant_status" in their
    metadata.

    """
    return search.bymetakey(keyquery)


@func('by.metaitem')
def bymetaitems(key: str, value: Union[str, Number]) -> search.query:
    """Yields a query filter operating on metadata items.

    Example: `(add (findseries (by.metaitem "plant_status" "running")))`

    This will filter the series having "running" as a value for the
    "plant_status" key in their metadata.

    """
    return search.bymetaitem(key, value)


@func('by.value')
def byvalue(key: str, operator: str, value: Union[str, Number]) -> search.query:
    """Yields a query filter operating on metadata items.

    Example: `(add (findseries (by.value "weigth" "<=" 42)))`

    This will filter the series having a "weight" metadata entry and
    keep those whose values is <= 42.

    The available operators are <, <=, >, >=.

    """
    assert operator in ('<', '<=', '>', '>=')
    if isinstance(value, str):
        valexpr = f'"{value}"'
    else:
        valexpr = f'{value}'
    return search.query.fromexpr(f'({operator} "{key}" {valexpr})')


@func('by.not')
def bynot(query: search.query) -> search.query:
    """Yields a query filter negating its input query filter.

    Example: `(add (findseries (by.not (by.name "capacity"))))`

    This will filter the series NOT having "capacity" in their name.

    """
    return search.not_(query)


@func('by.and')
def byand(*queries: search.query) -> search.query:
    """Yields a query filter doing a logical AND to its input query
    filters.

    Example: `(add (findseries (by.and (by.name "capacity") (by.metakey "plant"))))`

    """
    return search.and_(*queries)


@func('by.or')
def byor(*queries: search.query) -> search.query:
    """Yields a query filter doing a logical OR to its input query
    filters.

    Example: `(add (findseries (by.or (by.name "capacity") (by.metakey "plant"))))`

    """
    return search.or_(*queries)


def asof_transform(tree):
    posargs, _kwargs = buildargs(tree[1:])
    return [
        Symbol('let'), Symbol('revision_date'), posargs[0], tree
    ]


@func('asof')
@argscope('asof', asof_transform)
def asof(revision_date: pd.Timestamp,
         series: pd.Series) -> pd.Series:
    """
    Fetch the series in the asof scope with the specified revision date.

    Example: `(asof (shifted (today) #:days -1) (series "i-have-many-versions"))`

    """
    return series


@func('tzaware-stamp')
def tzaware_date(dt: pd.Timestamp, tzone: str) -> pd.Timestamp:
    if dt is None:
        return
    if dt.tzinfo is None:
        return pd.Timestamp(dt, tz=tzone)
    # not naive, we don't touch it
    return dt.tz_convert(tzone)


def dedupe(series):
    if series.index.duplicated().any():
        return series.groupby(series.index).mean()
    return series


def naive_transform(tree):
    posargs, _kwargs = buildargs(tree[1:])
    tzone = posargs[-1]

    top = [Symbol('let')]
    for name in ('from_value_date', 'to_value_date'):
        top += [Symbol(name),
                [Symbol('tzaware-stamp'), Symbol(name), tzone]]

    top.append(tree)
    return top


@func('naive')
@argscope('naive', naive_transform)
def naive(series: pd.Series, tzone: str) -> pd.Series:
    """
    Allow demoting a series from a tz-aware index to a tz-naive index.

    One must provide a target timezone.

    Example: `(naive (series "tz-aware-series-from-poland") "Europe/Warsaw")`

    """
    if not len(series):
        return empty_series(False)

    if not tzaware_serie(series):
        return dedupe(series)

    series.index = series.index.tz_convert(tzone).tz_localize(None)
    return dedupe(series)


@func('date')
def timestamp(strdate: str,
              tz: Optional[str]='UTC') -> pd.Timestamp:
    """
    Produces an utc timestamp from its input string date in iso format.

    The `tz` keyword allows to specify an alternate time zone.
    The `naive` keyword forces production of a naive timestamp.
    Both `tz` and `naive` keywords are mutually exlcusive.
    """
    if tz:
        pytz.timezone(tz)
    if tz is None:
        return pd.Timestamp(strdate)
    return pd.Timestamp(strdate, tz=tz)


@func('shifted')
def shifted(date: pd.Timestamp,
            years: int=0,
            months: int=0,
            weeks: int=0,
            days: int=0,
            hours: int=0,
            minutes: int=0) -> pd.Timestamp:
    """
    Takes a timestamp and a number of years, months, weekds, days,
    hours, minutes (int) and computes a new date according to the asked
    delta elements.

    Example: `(shifted (date "2020-1-1") #:weeks 1 #:hours 2)`

    """
    if date is None:
        return None
    return date + relativedelta(
        years=years,
        months=months,
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=minutes
    )


@func('today')
def today(__interpreter__,
          naive: Optional[bool]=False,
          tz: Optional[str]=None) -> pd.Timestamp:
    """
    Produces a timezone-aware timestamp as of today

    The `naive` keyword forces production of a naive timestamp.
    The `tz` keyword allows to specify an alternate time zone
    (if unpecified and not naive).
    Both `tz` and `naive` keywords are mutually exlcusive.

    Example: `(today)`
    """
    # impl. note: if not naive and tz is none,
    # tz will be utc
    return __interpreter__.today(naive, tz)


@func('start-of-month')
def start_of_month(date: pd.Timestamp) -> pd.Timestamp:
    """
    Produces a timezone-aware timestamp equal to the first day of the
    given date current month.

    Example: `(start-of-month (date "1973-05-20 09:00"))`

    """
    return date.replace(day=1)


@func('end-of-month')
def end_of_month(date: pd.Timestamp) -> pd.Timestamp:
    """
    Produces a timezone-aware timestamp equal to the last day of the
    given date current month.

    Example: `(end-of-month (date "1973-05-20 09:00"))`

    """
    _, end = calendar.monthrange(date.year, date.month)
    return date.replace(day=end)


@func('constant', auto=True)
def constant(__interpreter__,
             __from_value_date__,
             __to_value_date__,
             __revision_date__,
             value: Number,
             fromdate: pd.Timestamp,
             todate: pd.Timestamp,
             freq: str,
             revdate: pd.Timestamp) -> pd.Series:
    """
    Produces a constant-valued timeseries over a pre-defined horizon
    and a given granularity and for a given revision date.

    Example: `(constant 42.5 (date "1900-1-1") (date "2039-12-31") "D" (date "1900-1-1"))`

    This will yield a daily series of value 42.5 between 1900 and
    2040, dated from 1900.

    """
    assert fromdate.tzinfo is not None
    assert todate.tzinfo is not None
    assert revdate.tzinfo is not None

    return _constant(__interpreter__,
                     {'revision_date': __revision_date__,
                      'from_value_date': __from_value_date__,
                      'to_value_date': __to_value_date__},
                     value, fromdate, todate, freq, revdate)


def _constant(__interpreter__, args, value, fromdate, todate, freq, revdate):
    getargs = __interpreter__.getargs
    qrevdate = args.get('revision_date')
    if qrevdate and ensuretz(qrevdate) < revdate:
        return empty_series(True)

    qfromidate = getargs.get('from_insertion_date')
    if qfromidate and ensuretz(qfromidate) > revdate:
        return empty_series(True)

    qtoidate = getargs.get('to_insertion_date')
    if qtoidate and ensuretz(qtoidate) < revdate:
        return empty_series(True)

    mindate = args.get('from_value_date')
    if mindate:
        mindate = ensuretz(mindate)

    maxdate = args.get('to_value_date')
    if maxdate:
        maxdate = ensuretz(maxdate)

    dates = pd.date_range(
        start=fromdate,
        end=todate,
        freq=freq
    )

    return pd.Series(
        [value] * len(dates),
        name='constant',
        index=dates,
        dtype='float64'
    ).loc[mindate:maxdate]


@metadata('constant')
def constant_metadata(cn, tsh, tree):
    return {
        'constant': {
        'tzaware': True,
        'index_type': 'datetime64[ns, UTC]',
        'value_type': 'float64',
        'index_dtype': '|M8[ns]',
        'value_dtype': '<f8'
        }
    }


@history('constant')
def constant_history(__interpreter__, value, fromdate, todate, freq, revdate):
    series = _constant(
        __interpreter__, __interpreter__.getargs, value, fromdate, todate, freq, revdate
    )
    if len(series):
        return {
            revdate: series
        }
    return {}


@insertion_dates('constant')
def constant_idates(cn, tsh, tree,
                    from_insertion_date=None, to_insertion_date=None,
                    from_value_date=None, to_value_date=None):
    itrp = Interpreter(cn, tsh, {})
    revdate = itrp.evaluate(tree[-1])
    if (from_insertion_date and
        from_insertion_date >= revdate or
        to_insertion_date and
        to_insertion_date <= revdate):
        return []
    return [revdate]


@func('+')
def scalar_add(
        num: Number,
        num_or_series: Union[Number, pd.Series]) -> Union[Number, pd.Series]:
    """
    Add a constant quantity to a series.

    Example: `(+ 42 (series "i-feel-undervalued"))`
    """
    opts = None
    if isinstance(num_or_series, pd.Series):
        assert isinstance(num, (int, float))
        opts = num_or_series.options

    res = num + num_or_series
    if opts is not None:
        # we did a series + scalar and want to propagate
        # the original series options
        res.options = opts
    return res


@func('*')
def scalar_prod(
        num: Number,
        num_or_series: Union[Number, pd.Series]) -> Union[Number, pd.Series]:
    """
    Performs a scalar product on a series.

    Example: `(* -1 (series "positive-things"))`
    """
    opts = None
    if isinstance(num_or_series, pd.Series):
        assert isinstance(num, (int, float))
        opts = num_or_series.options

    res = num * num_or_series
    if opts is not None:
        # we did a series * scalar and want to propagate
        # the original series options
        res.options = opts
    return res


@func('/')
def scalar_div(
        num_or_series: Union[Number, pd.Series],
        num: Number) -> Union[Number, pd.Series]:
    """
    Perform a scalar division between numbers or a series and a scalar.

    Example: `(/ (series "div-me") (/ 3 2))`
    """
    opts = None
    if isinstance(num_or_series, pd.Series):
        assert isinstance(num, (int, float))
        opts = num_or_series.options

    res = num_or_series / num
    if opts is not None:
        res.options = opts
    return res


@func('**')
def scalar_pow(
        series: pd.Series,
        num: Number) -> pd.Series:
    """
    Performs an exponential power on a series.

    Example: `(** (series "positive-things") 2)`
    """
    opts = series.options

    res = series ** num
    res.options = opts
    return res



def _fill(df, colname, fillopt):
    """ in-place application of the series fill policies
    which can be a int/float or a coma separated string
    like e.g. 'ffill,bfill'
    """
    filler = fillopt['fill']
    limit = fillopt.get('limit')
    if isinstance(filler, str):
        for method in filler.split(','):
            df[colname] = df[colname].fillna(
                method=method.strip(),
                limit=limit
            )
    elif isinstance(filler, (int, float)):
        df[colname] = df[colname].fillna(
            value=filler,
            limit=limit
        )


def _group_series(*serieslist):
    dfs = []
    opts = {}

    # join everything
    for idx, ts in enumerate(serieslist):
        if ts.options.get('fill') is None and not len(ts):
            # at least one series without fill policy and no data
            # entails an empty result
            return pd.DataFrame(dtype='float64')

        ts.name = f'{idx}'  # do something unique
        opts[ts.name] = ts.options
        dfs.append(ts)

    df = pd.concat(dfs, axis=1, join='outer')

    # apply the filling rules
    for name, fillopt in opts.items():
        if fillopt:
            _fill(df, name, fillopt)

    return df


# trigonometric functions

@func('trig.cos')
def trig_cosinus(series: pd.Series) -> pd.Series:
    """
    Cosine element-wise on a degree series.

    Example: `(trig.cos (series "degree-series"))`
    """
    opts = series.options
    res = np.cos(series * (np.pi / 180))
    res.options = opts
    return res


@func('trig.arccos')
def trig_arccosinus(series: pd.Series) -> pd.Series:
    """
    Trigonometric inverse cosine on a series of values [-1, 1] with a degree output.

    Example: `(trig.arcos (series "coordinates"))`
    """
    opts = series.options
    res = np.arccos(series) * (180 / np.pi)
    res = res.dropna()
    res.options = opts
    return res


@func('trig.sin')
def trig_sinus(series: pd.Series) -> pd.Series:
    """
    Trigonometric sine element-wise on a degree series.

    Example: `(trig.sin (series "degree-series"))`
    """
    opts = series.options

    res = np.sin(series * (np.pi / 180))
    res.options = opts
    return res


@func('trig.arcsin')
def trig_arcsinus(series: pd.Series) -> pd.Series:
    """
    Trigonometric inverse sine on a series of values [-1, 1] with a degree output.

    Example: `(trig.arcsin (series "coordinates"))`
    """
    opts = series.options
    res = np.arcsin(series) * (180 / np.pi)
    res = res.dropna()
    res.options = opts
    return res


@func('trig.tan')
def trig_tangent(series: pd.Series) -> pd.Series:
    """
    Compute tangent element-wise on a degree series.

    Example: `(trig.tan (series "degree-series"))`
    """
    opts = series.options

    res = np.tan(series * (np.pi / 180))
    res.options = opts
    return res


@func('trig.arctan')
def trig_arctangent(series: pd.Series) -> pd.Series:
    """
    Trigonometric inverse tangent on a series of values [-1, 1] with a degree output.

    Example: `(trig.arctan (series "coordinates"))`
    """
    opts = series.options
    res = np.arctan(series) * (180 / np.pi)
    res.options = opts
    return res


@func('trig.row-arctan2')
def trig_arctangent2(series1: pd.Series,
                     series2: pd.Series) -> pd.Series:
    """
    Arc tangent of x1/x2 choosing the quadrant correctly with a degree output.

    Example: `(trig.row-arctan2 (series "coordinates1") (series "coordinates2"))`
    """
    df = _group_series(series1, series2)
    if not len(df):
        return empty_series(
            tzaware_serie(series1)
        )
    res = np.arctan2(df['0'], df['1']) * (180 / np.pi)
    return res.dropna()


# /trigo

# conditionals operators

def _comparator(
    operation,
    series,
    num_or_series,
    false_value,
    true_value,
):
    if isinstance(num_or_series, pd.Series):
        # pandas only allows comparison of series with the same index
        # hence we restrict each series to their commmon part
        intersect = series.index.intersection(num_or_series.index)
        if not len(intersect):
            return empty_series(tzaware_serie(series))
        series = series[intersect]
        num_or_series = num_or_series[intersect]

    if operation == '<':
        mask = series < num_or_series
    elif operation == '<=':
        mask = series <= num_or_series
    elif operation == '>':
        mask = series > num_or_series
    elif operation == '>=':
        mask = series >= num_or_series
    elif operation == '==':
        mask = series == num_or_series
    elif operation == '<>':
        mask = series != num_or_series

    ts = pd.Series(index=mask.index, dtype='float64')
    ts[mask] = true_value
    ts[~mask] = false_value
    return ts


@func('>')
def superior_to(series: pd.Series,
                num_or_series: Union[Number, pd.Series],
                false_value: Optional[Number] = 0,
                true_value: Optional[Number] = 1) -> pd.Series:
    """
    Returns a series of length of the first argument if the second one
    is a scalar, or of the length of the index intersection in the case
    of two series.

    Series values are dependent on the condition:
    * the values are set to true_value (default 1) where the condition is verified.
    * the values are set to false_value (default 0) where the condition is NOT verified.
    """
    return _comparator('>', series, num_or_series, false_value, true_value)


@func('>=')
def superior_or_equal_to(series: pd.Series,
                         num_or_series: Union[Number, pd.Series],
                         false_value: Optional[Number] = 0,
                         true_value: Optional[Number] = 1) -> pd.Series:
    """
    Returns a series of length of the first argument if the second one
    is a scalar, or of the length of the index intersection in the case
    of two series.

    Series values are dependent on the condition:
    * the values are set to true_value (default 1) where the condition is verified.
    * the values are set to false_value (default 0) where the condition is NOT verified.
    """
    return _comparator('>=', series, num_or_series, false_value, true_value)


@func('<')
def inferior_to(series: pd.Series,
                num_or_series: Union[Number, pd.Series],
                false_value: Optional[Number] = 0,
                true_value: Optional[Number] = 1) -> pd.Series:
    """
    Returns a series of length of the first argument if the second one
    is a scalar, or of the length of the index intersection in the case
    of two series.

    Series values are dependent on the condition:
    * the values are set to true_value (default 1) where the condition is verified.
    * the values are set to false_value (default 0) where the condition is NOT verified.
    """
    return _comparator('<=', series, num_or_series, false_value, true_value)


@func('<=')
def inferior_or_equal_to(series: pd.Series,
                         num_or_series: Union[Number, pd.Series],
                         false_value: Optional[Number] = 0,
                         true_value: Optional[Number] = 1) -> pd.Series:
    """
    Returns a series of length of the first argument if the second one
    is a scalar, or of the length of the index intersection in the case
    of two series.

    Series values are dependent on the condition:
    * the values are set to true_value (default 1) where the condition is verified.
    * the values are set to false_value (default 0) where the condition is NOT verified.
    """
    return _comparator('<=', series, num_or_series, false_value, true_value)


@func('==')
def equal_to(series: pd.Series,
             num_or_series: Union[Number, pd.Series],
             false_value: Optional[Number] = 0,
             true_value: Optional[Number] = 1) -> pd.Series:
    """
    Returns a series of length of the first argument if the second one
    is a scalar, or of the length of the index intersection in the case
    of two series.

    Series values are dependent on the condition:
    * the values are set to true_value (default 1) where the condition is verified.
    * the values are set to false_value (default 0) where the condition is NOT verified.
    """
    return _comparator('==', series, num_or_series, false_value, true_value)


@func('<>')
def different_to(series: pd.Series,
                 num_or_series: Union[Number, pd.Series],
                 false_value: Optional[Number] = 0,
                 true_value: Optional[Number] = 1) -> pd.Series:
    """
    Returns a series of length of the first argument if the second one
    is a scalar, or of the length of the index intersection in the case
    of two series.

    Series values are dependent on the condition:
    * the values are set to true_value (default 1) where the condition is verified.
    * the values are set to false_value (default 0) where the condition is NOT verified.
    """
    return _comparator('<>', series, num_or_series, false_value, true_value)

# /conditionals


@func('add')
def series_add(*serieslist: pd.Series) -> pd.Series:
    """
    Linear combination of two or more series. Takes a variable number
    of series as input.

    Example: `(add (series "wallonie") (series "bruxelles") (series "flandres"))`

    To specify the behaviour of the `add` operation in the face of
    missing data, the series can be built with the `fill`
    keyword. This option is only really applied when several series
    are combined. By default, if an input series has missing values
    for a given time stamp, the resulting series has no value for this
    timestamp (unless a fill rule is provided).

    """
    if not serieslist:
        # by default, yield an empty timezone aware series
        # ideally, we'd get the tzaware bit from the context
        # but we don't yet have that
        return empty_series(True)

    assert [
        isinstance(s, pd.Series)
        for s in serieslist
    ]

    return _group_series(*serieslist).dropna().sum(axis=1)


@func('mul')
def series_multiply(*serieslist: pd.Series) -> pd.Series:
    """
    Element wise multiplication of series. Takes a variable number of
    series as input.

    Example: `(mul (series "banana-spot-price ($)") (series "$-to-€" #:fill "ffill"))`

    This might convert a series priced in dollars to a series priced
    in euros, using a currency exchange rate series with a
    forward-fill option.
    """
    df = _group_series(*serieslist)
    if not len(df):
        return empty_series(
            tzaware_serie(serieslist[0])
        )

    res = None
    for col in df.columns:
        if res is None:
            res = df[col].to_frame()
            continue
        res = res.multiply(df[col], axis=0)

    return res[res.columns[0]].dropna()


@func('div')
def series_div(s1: pd.Series, s2: pd.Series) -> pd.Series:
    """
    Element wise division of two series.

    Example: `(div (series "$-to-€") (series "€-to-£"))`
    """
    df = _group_series(*(s1, s2))
    if not len(df) or len(df.columns) < 2:
        return empty_series(
            tzaware_serie(s1)
        )

    c1, c2 = df.columns
    return (df[c1] / df[c2]).dropna()


@func('priority')
def series_priority(*serieslist: pd.Series) -> pd.Series:
    """
    The priority operator combines its input series as layers. For
    each timestamp in the union of all series time stamps, the value
    comes from the first series that provides a value.

    Example: `(priority (series "realized") (series "nominated") (series "forecasted"))`

    Here `realized` values show up first, and any missing values come
    from `nominated` first and then only from `forecasted`.
    """
    if len(serieslist) == 1:
        return serieslist[0]

    series = list(serieslist)
    series.reverse()
    return patchmany(series)


@func('clip')
def series_clip(series: pd.Series,
                min: Optional[Number]=None,
                max: Optional[Number]=None,
                replacemin: Optional[bool]=False,
                replacemax: Optional[bool]=False) -> pd.Series:
    """
    Set an upper/lower threshold for a series. Takes a series as
    positional parameter and accepts four optional keywords `min` and
    `max` which must be numbers, `replacemin` and
    `replacemax` to control filling out of bounds data with min and
    max respectively.

    Example: `(clip (series "must-be-positive") #:min 0 #:replacemin #t)`

    """
    options = series.options.copy()
    if max is not None:
        mask = series <= max
        if replacemax:
            series[~mask] = max
        else:
            series = series[mask]
    if min is not None:
        mask = series >= min
        if replacemin:
            series[~mask] = min
        else:
            series = series[mask]
    series.options = options
    return series


def slice_transform(tree):
    _posargs, kwargs = buildargs(tree[1:])
    qargs = {}
    for treeparam, targetparam in (
            ('fromdate', 'from_value_date'),
            ('todate', 'to_value_date')
    ):
        if treeparam in kwargs:
            qargs[targetparam] = kwargs[treeparam]

    if not qargs:
        # nothing to transform
        return tree

    argfunc = {
        'from_value_date': 'max',
        'to_value_date': 'min',
        'revision_date': None
    }
    top = [Symbol('let')]
    for name, value in qargs.items():
        func = argfunc[name]
        top += [Symbol(name),
                [Symbol(func), Symbol(name), value] if func else value]

    top.append(tree)
    return top


@func('slice')
@argscope('slice', slice_transform)
def slice(series: pd.Series,
          fromdate: Optional[pd.Timestamp]=None,
          todate: Optional[pd.Timestamp]=None) -> pd.Series:
    """
    This allows cutting a series at date points. It takes one
    positional parameter (the series) and two optional keywords
    `fromdate` and `todate`.

    Example: `(slice (series "cut-me") #:fromdate (date "2018-01-01"))`
    """
    # the series operator did request with fromdate/todate
    # because of our `scope` hint
    # hence we a little to do
    return series


@func('row-mean')
def row_mean(*serieslist: pd.Series, skipna: Optional[bool]=True) -> pd.Series:
    """
    This operator computes the row-wise mean of its input series using
    the series `weight` option if present. The missing points are
    handled as if the whole series were absent.

    Example: `(row-mean (series "station0") (series "station1" #:weight 2) (series "station2"))`

    Weights are provided as a keyword to `series`. No weight is
    interpreted as 1.
    """
    weights = [
        series.options.get('weight', 1)
        for series in serieslist
    ]

    allseries = pd.concat(serieslist, axis=1)
    weights_in_vertical_matrix = np.array(
        [[w] for w in weights]
    )
    if skipna:
        weighted_sum = allseries.fillna(0).values.dot(
            weights_in_vertical_matrix
        )
    else:
        weighted_sum = allseries.values.dot(
            weights_in_vertical_matrix
        )
    denominator = (~allseries.isnull()).values.dot(
        weights_in_vertical_matrix
    )

    return pd.Series(
        (weighted_sum / denominator).flatten(),
        index=allseries.index
    ).dropna()


@func('row-min')
def row_min(*serieslist: pd.Series, skipna: Optional[bool]=True) -> pd.Series:
    """
    Computes the row-wise minimum of its input series.

    Example: `(row-min (series "station0") (series "station1") (series "station2"))`

    Example: `(row-min (series "station0") (series "station1") #:skipna #f)`

    The `skipna` keyword (which is true by default) controls the
    behaviour with nan values.
    """
    allseries = pd.concat(serieslist, axis=1)
    return allseries.min(axis=1, skipna=skipna).dropna()


@func('row-max')
def row_max(*serieslist: pd.Series, skipna: Optional[bool]=True) -> pd.Series:
    """
    Computes the row-wise maximum of its input series.

    Example: `(row-max (series "station0") (series "station1") (series "station2"))`

    Example: `(row-max (series "station0") (series "station1") #:skipna #f)`

    The `skipna` keyword (which is true by default) controls the
    behaviour with nan values.
    """
    allseries = pd.concat(serieslist, axis=1)
    return allseries.max(axis=1, skipna=skipna).dropna()


@func('std')
def row_std(*serieslist: pd.Series, skipna: Optional[bool]=True) -> pd.Series:
    """
    Computes the standard deviation over its input series.

    Example: `(std (series "station0") (series "station1") (series "station2"))`

    Example: `(std (series "station0") (series "station1") #:skipna #f)`

    The `skipna` keyword (which is true by default) controls the
    behaviour with nan values.
    """
    allseries = pd.concat(serieslist, axis=1)
    return allseries.std(axis=1, skipna=skipna).dropna()


def resample_adjust(tstamp, freq):
    """Return adjusted time stamp when resampling with freq.

    Examples:
        >>> resample_interval_start(pd.Timestamp('2021-01-01 12:31'), 'D')
        pd.Timestamp('2021-01-01')
        >>> resample_interval_start(pd.Timestamp('2021-01-01 12:31'), 'H')
        pd.Timestamp('2021-01-01 12:00')
    """
    return next(iter(pd.Series([0], index=[tstamp]).resample(freq).groups))


@func('resample_adjusted_stamp')
def resample_adjusted_stamp(
        tstamp: Optional[pd.Timestamp],
        freq: str,
        direction: str) -> Optional[pd.Timestamp]:
    # Return timestamp compensated to account for the resampling
    # operation (we must provide enough data outside the strict query
    # range for the operation to not yield bad results).

    assert direction in ('left', 'right')
    if tstamp is None:
        return tstamp

    interval_start = resample_adjust(tstamp, freq)
    if direction == 'left':
        return interval_start

    # workaround: Retrieve 1 microsecond to left endpoint to force
    # half-open interval - necessary because to_value_date is included
    # in tsio.timeseries.get see test.test_resample
    return (
        interval_start
        + pd.tseries.frequencies.to_offset(freq)
        + relativedelta(microseconds=-1)
    )


def resample_transform(tree):
    """Enlarge the from_value_date <-> to_value_date range to get
    consistent resampling data

    pandas resampling creates intervals based on origin (by default
    'start_day') For instance, when resampling series starting on
    '2000-01-02 08:37': - if freq='D', first interval is ['2000-01-02
    00:00', '2000-01-03 00:00'] - if freq='H', first interval is
    ['2000-01-02 08:00', '2000-01-02 09:00']

    So number of points in the first and last intervals tends to be
    smaller.

    For instance a series with one point per minute and starting at
    '08:37' will have 23 points in the first interval when resampled
    to the hour.

    To solve this issue, we enlarge the retrieval boundaries of the
    series that is resampled.

    For instance, in the previous example, from_value_date will be
    moved from '08:37' to '08:00' for an hourly resampling.

    """
    _posargs, kwargs = buildargs(tree[1:])
    freq = _posargs[1]
    top = [
        Symbol('let'),
        Symbol('from_value_date'), [
            Symbol('resample_adjusted_stamp'),
            Symbol('from_value_date'),
            freq,
            'left',
        ],
        Symbol('to_value_date'), [
            Symbol('resample_adjusted_stamp'),
            Symbol('to_value_date'),
            freq,
            'right',
        ],
    ]
    top.append(tree)
    return top


@func('resample')
@argscope('resample', resample_transform)
def resample(series: pd.Series,
             freq: str,
             method: str='mean') -> pd.Series:
    """
    Resamples its input series using `freq` and the aggregation method
    `method` (as described in the pandas documentation).

    Example: `(resample (series "hourly") "D")`

    """
    if not len(series):
        return series

    resampled = series.resample(freq)

    # check method
    meth = getattr(resampled, method, None)
    if meth is None:
        raise ValueError(f'bad resampling method `{method}`')

    return resampled.apply(method).dropna()


@func('cumsum')
def cumsum(series: pd.Series) -> pd.Series:
    """
    Return cumulative sum over a series.

    Example: `(cumsum (series "sum-me"))`

    """

    return series.cumsum()


def time_shifted_transform(tree):
    _posargs, kwargs = buildargs(tree[1:])

    if not kwargs:
        # the signature of time-shifted allows a no-kw
        # call (meaningless but possible)
        return tree[1]

    def negate(items):
        return [
            -x if isinstance(x, (int, float)) else x
            for x in items
        ]

    top = [Symbol('let')]
    for name in ('from_value_date', 'to_value_date'):
        top += [
            Symbol(name),
            [Symbol('shifted'), Symbol(name)] +
            negate(
                reduce(operator.add, kwargs.items())
            )
        ]

    top.append(tree)
    return top


@func('time-shifted')
@argscope('time-shifted', time_shifted_transform)
def time_shifted(series: pd.Series,
                 weeks: int=0,
                 days: int=0,
                 hours: int=0,
                 minutes: int=0) -> pd.Series:
    """
    Shift the dates of a series.

    Takes the following keywords: `weeks`, `days`, `hours`, with
    positive or negative values.

    Example `(time-shifted (series "shifted") #:days 2 #:hours 7)`

    """
    # note: relativedelta is unfit there as it cannot be
    # broadcast on the index
    series.index = series.index + timedelta(
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=minutes
    )

    return series


@func('rolling')
def rolling(series: pd.Series,
            window: int,
            method: str='mean') -> pd.Series:
    """
    Computes a calculation `method` (mean by default) to a rolling
    window (as described in the pandas documentation).

    Example: `(rolling (series "foo") 30 #:method "median"))`

    """
    if not len(series):
        return series

    rolled = series.rolling(window)
    df = rolled.agg((method,)).dropna()
    return df[df.columns[0]]


# integration -- a somewhat hairy operator :)
# keep me at the end of this module

def any_hole(stock, flow):
    # sotck and flow are index
    # we only scan the period where there are stock
    first_stock = min(stock)
    last_stock = max(stock)
    flow = flow[(flow > first_stock) & (flow < last_stock)]
    return not flow.isin(stock).all()


def compute_bounds(stock, flow):
    """
    stock and flow are timestamp indexes
    we compute the intervals (start, stop) for which
    there is stock, or else flow
    """
    # case where the stock goes further in the future than the flow
    stock = stock[stock <= max(flow)]

    all_dates = stock.union(flow).sort_values()
    is_stock = pd.Series(
        all_dates.isin(stock),
        index=all_dates
    ).astype(int)

    variation = is_stock.diff()
    # the first value is NaN and we force it as a stock bucket start
    variation.iloc[0] = 1
    starts_stock = variation == 1
    starts_flow = variation == -1

    ends_stock = starts_flow.index[
        starts_flow.shift(-1).fillna(False)
    ]
    ends_flow = starts_stock.index[
        starts_stock.shift(-1).fillna(False)
    ]

    starts_stock = starts_stock.index[
        starts_stock
    ]
    starts_flow = starts_flow.index[
        starts_flow
    ]
    if not is_stock.iloc[-1]:
        # the last value is a flow
        ends_flow = ends_flow.append(is_stock.index[-1:])
    else:
        # the last value is a stock
        ends_stock = ends_stock.append(is_stock.index[-1:])
        starts_flow = starts_flow.append(pd.DatetimeIndex([None]))
        ends_flow = ends_flow.append(pd.DatetimeIndex([None]))

    assert len(starts_flow) == len(starts_stock) == len(ends_flow) == len(ends_stock)

    return zip(
        starts_stock,
        ends_stock,
        starts_flow,
        ends_flow,
    )


def find_last_values(
        __interpreter__,
        name,
        revision_date,
        from_value_date,
        to_value_date,
        fill,
        tzaware):
    """
    Returns a series contained between from_value_date and
    to_value_date If no data is found in this interval, this function
    will look at the left of the lower bound until it finds something
    (with a hard-coded limit to avoid infinite loops)
    """
    period = timedelta(days=10)
    multiplier = 2

    args = {}
    if revision_date:
        args['revision_date'] = revision_date

    if to_value_date:
        args.update({'to_value_date': to_value_date})

    if not from_value_date:
        return __interpreter__.get(name, args)

    current_bound = from_value_date
    ts = empty_series(tzaware)
    if not fill:
        while not len(ts):
            args.update({'from_value_date': current_bound})
            ts = __interpreter__.get(name, args)
            current_bound = from_value_date - period
            period = period * multiplier
            if period > timedelta(days=3000):
                break
    else:
        args.update({'from_value_date': from_value_date})
        ts = __interpreter__.get(name, args)
        previous_ts = empty_series(tzaware)
        current_bound = from_value_date
        args.update({'to_value_date': from_value_date})
        while not len(previous_ts):
            args.update({'from_value_date': current_bound})
            previous_ts = __interpreter__.get(name, args)
            current_bound = from_value_date - period
            period = period * multiplier
            if period > timedelta(days=3000):
                break
        if len(previous_ts):
            ts = patch(previous_ts, ts)

    return ts


def _integration(__interpreter__, iargs, stock_name, flow_name, fill):
    i = __interpreter__
    args = iargs.copy()
    from_value_date = args.pop('from_value_date', None)
    to_value_date = args.pop('to_value_date', None)

    assert i.tsh.exists(i.cn, stock_name), f'No series {stock_name}'
    assert i.tsh.exists(i.cn, flow_name), f'No series {flow_name}'

    tzaware = i.tsh.internal_metadata(i.cn, stock_name)['tzaware']
    ts_stock = find_last_values(
        __interpreter__,
        stock_name,
        args.get('revision_date'),
        from_value_date,
        to_value_date,
        fill,
        tzaware
    )
    ts_stock = ts_stock.dropna()
    if from_value_date:
        from_value_date = compatible_date(
            tzaware,
            from_value_date
        )

    if to_value_date:
        to_value_date = compatible_date(
            tzaware,
            to_value_date
        )

    if not len(ts_stock):
        return empty_series(tzaware)

    if not fill:
        first_diff_date = ts_stock.index[-1]
    elif from_value_date is None:
        first_diff_date = ts_stock.index[0]
    else:
        dates_stock_before = ts_stock.index < from_value_date
        if dates_stock_before.any():
            first_diff_date = max(ts_stock.index[ts_stock.index < from_value_date])
        else:
            first_diff_date = ts_stock.index[0]

    args.update({'from_value_date': first_diff_date})
    if to_value_date:
        args.update({'to_value_date': to_value_date})

    if to_value_date and to_value_date < first_diff_date:
        ts_diff = empty_series(tzaware)
    else:
        ts_diff = __interpreter__.get(flow_name, args)

    # we want to exclude the first value which is a stock value
    # and from_value_date is inclusive
    ts_diff = ts_diff[ts_diff.index > first_diff_date]

    if ts_diff is None or not len(ts_diff):
        ts_total = ts_stock
    else:
        if to_value_date:
            ts_diff = ts_diff[ts_diff.index <= to_value_date]
        if fill:
            if any_hole(ts_stock.index, ts_diff.index):
                bounds = compute_bounds(ts_stock.index, ts_diff.index)
            else:
                return _integration(
                    __interpreter__,
                    iargs,
                    stock_name,
                    flow_name,
                    fill=False
                )
        else:
            bounds = [
                (
                    ts_stock.index[0],
                    ts_stock.index[-1],
                    ts_diff.index[0],
                    ts_diff.index[-1]
                )
            ]
        chunks = []
        for start_stock, end_stock, start_diff, end_diff in bounds:
            stock = ts_stock.loc[start_stock: end_stock]
            diff = ts_diff.loc[start_diff: end_diff]
            chunks.append(stock)
            chunks.append(diff.cumsum(skipna=False) + stock.iloc[-1])
        ts_total = pd.concat(chunks)

    if from_value_date:
        ts_total = ts_total[ts_total.index >= from_value_date]
    if to_value_date:
        ts_total = ts_total[ts_total.index <= to_value_date]

    return ts_total


@func('integration')
def integration(
        __interpreter__,
        __from_value_date__,
        __to_value_date__,
        __revision_date__,
        stock_name: str,
        flow_name: str,
        fill: Optional[bool]=False) -> pd.Series:
    """
    Integrate a given flow series to the last known value of a stock series.

    Example: `(integration "stock-series-name" "flow-series-name")`
    """
    args = {
        'from_value_date': __from_value_date__,
        'to_value_date': __to_value_date__,
        'revision_date': __revision_date__
    }

    return _integration(
        __interpreter__,
        args,
        stock_name,
        flow_name,
        fill
    )


@metadata('integration')
def integration_metadata(cn, tsh, tree):
    return {
        tree[1]: tsh.internal_metadata(cn, tree[1]),
        tree[2]: tsh.internal_metadata(cn, tree[2])
    }


@finder('integration')
def integration_finder(cn, tsh, tree):
    return {
        tree[1]: tree,
        tree[2]: tree
    }


# day of year

def doy_scope_shift_transform(tree):
    """Shift from_value_date/to_value_date when gathering series for doy-aggregation"""
    _posargs, kwargs = buildargs(tree[1:])
    depth = _posargs[1]
    top = [
        Symbol('let'),
        Symbol('from_value_date'), [
            Symbol('shifted'), Symbol('from_value_date'), Keyword('years'), -int(depth)
        ],
        Symbol('to_value_date'), [
            Symbol('shifted'), Symbol('to_value_date'), Keyword('years'), -1
        ],
    ]
    top.append(tree)
    return top


@func('doy-agg')
@argscope('doy-agg', doy_scope_shift_transform)
def doy_aggregation(
        series: pd.Series,
        depth: int,
        method: str = "mean",
        leap_day_rule: str = "linear",  # Literal["ignore", "linear", "as_is"]
        valid_aggr_ratio: Number = 1.) -> pd.Series:
    """
    Computes a calculation `method` to aggregate data per day of year over `depth` years.

    Examples:

     `(doy-agg (series "foo") 4)`

     `(doy-agg (series "bar") 10 #:method "median")`

     `(doy-agg (series "quux") 4 #:leap_day_rule "ignore" #:valid_aggr_ratio 0.)`

    The `method` keyword controls the function of aggregation (see
    pandas DataFrameGroupBy aggregate).

    The `leap_day_rule` keyword controls the policy to handle the leap day (February 29)
        'as_is'     build leap year using aggregation of previous leap days
        'ignore'    don't build leap day
        'linear'    build leap day using the closest dates with linear extrapolation

    The `valid_aggr_ratio` keyword controls the minimum ratio
    (number_of_values_for_doy / depth) for a given day of year to be
    valid (not set to nan).

    For instance:
       one is asking an aggregation over 4 previous years (depth=4)
       for the January 1st (doy="01-01"), we have only 2 values over the period 2020-2023
       then aggr_ratio for 2024-01-01 is 2/4=0.5
       meaning value will be nan if valid_aggr_ratio < 0.5

    """
    try:
        start, end = get_boundaries(series, depth)
    except ValueError:
        return empty_series(False)
    assert leap_day_rule in ["ignore", "linear", "as_is"]
    # L.info(
    #     f"doy-agg from {start} to {end} ["
    #     f" aggregator={method!r}"
    #     f" leap_day_rule={leap_day_rule!r}"
    #     f" valid_aggr_ratio={valid_aggr_ratio!r}"
    #     f"]"
    # )

    habits_segments = []
    for year in range(start.year, end.year + 1):
        # L.info(f'Computing habits for year {year}')
        # L.debug(f'aggregate by day of year')
        doy_agg = aggregate_by_doy(
            series,
            from_year=year - depth,
            to_year=year - 1,
            method=method,
        )
        doy_agg['ratio'] = doy_agg['values_count'] / depth
        # L.debug(f'replace insufficient points with nans [')
        doy_agg.loc[doy_agg['ratio'] < valid_aggr_ratio, series.name] = np.nan
        if (
            not calendar.isleap(year)
            or leap_day_rule in ('ignore', 'linear')
        ):
            # L.debug('drop leap day if exists')
            doy_agg = doy_agg.drop(labels=['02-29'], errors='ignore')

        segment = doy_agg[series.name]
        # L.debug('build index from days of year')
        segment.index = pd.to_datetime(
            str(year) + '-' + segment.index.to_series(), format='%Y-%m-%d'
        ).rename('datetime')
        segment = segment[
            (start <= segment.index)
            & (segment.index <= end)
        ]

        if leap_day_rule == 'linear' and calendar.isleap(year):
            # L.debug('extrapolate leap day')
            linear_insert_date(segment, pd.Timestamp(f'{year}-2-29'))

        # L.debug(f'add segment of {len(segment)} points')
        habits_segments.append(segment)

    # L.info(f'Concatenate and return {len(habits_segments)} segments')
    return pd.concat(habits_segments).dropna()


def aggregate_by_doy(
        series: pd.Series,
        from_year: int,
        to_year: int,
        method: str = 'mean') -> pd.DataFrame:
    """
    Return aggregation of data by day of year
        pd.DataFrame with
            index                   day_of_year  ("MM-DD")
            column '{series.name}'  aggregated data for that day of year
            column 'values_count'   number of years with data on that day
    """
    df = series.to_frame()
    df = df.loc[
        pd.Timestamp(f'{from_year}-1-1', tz=series.index.tz):
        pd.Timestamp(f'{to_year}-12-31', tz=series.index.tz)
    ]
    df['not_na'] = series.notna()
    df['day_of_year'] = df.index.strftime('%m-%d')
    return df.groupby('day_of_year').aggregate(
        {
            series.name: method,
            'not_na': sum,
        }
    ).rename({'not_na': 'values_count'}, axis=1)


def get_boundaries(
        series: pd.Series,
        depth: int) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Return boundaries on which doy-aggregation over {depth} years can
    be computed

    """
    if depth <= 0:
        raise ValueError('depth must be integer strictly greater than 0')

    if series.empty:
        raise ValueError('series is empty')

    fst_date: date = series.index[0].date()
    lst_date: date = series.index[-1].date()

    start = fst_date + relativedelta(years=depth)
    end = lst_date + relativedelta(years=1)
    if start > end:
        raise ValueError(
            f'series boundaries [{fst_date}, {lst_date}] '
            f'are not wide enough to aggregate '
            f'per day of years on {depth} years'
        )
    return pd.Timestamp(start), pd.Timestamp(end)


def linear_insert_date(series: pd.Series, d: datetime) -> pd.Series:
    """
    Insert date in series (inplace) using linear extrapolation with
    surrounding values

    """
    if d in series:
        return series

    try:
        prev_date = series.loc[:d].index[-1]
        next_date = series.loc[d + timedelta(days=1):].index[0]
    except IndexError:
        # No surrounding value, can't insert date
        return series

    prev_gap = (d - prev_date).days
    next_gap = (next_date - d).days

    series.at[d] = (
        series.loc[prev_date] * next_gap
        + series.loc[next_date] * prev_gap
    ) / (prev_gap + next_gap)
    series.sort_index(inplace=True)
    return series


# block staircase

def build_argdict_delta(arg_days, arg_hours):
    if arg_days is None and arg_hours is None:
        return None
    if arg_hours is None:
        return {'days': arg_days}
    if arg_days is None:
        return {'hours': arg_hours}
    else:
        return {'days': arg_days, 'hours': arg_hours}


def build_argdict(arg_days, arg_hours):
    if arg_days is None and arg_hours is None:
        return None
    if arg_hours is None:
        return {'day': arg_days}
    if arg_days is None:
        return {'hour': arg_hours}
    else:
        return {'day': arg_days, 'hour': arg_hours}


@func('block-staircase')
def block_staircase(__interpreter__,
                    __from_value_date__,
                    __to_value_date__,
                    name: seriesname,
                    revision_freq_hours: Optional[int] = None,
                    revision_freq_days: Optional[int] = None,
                    revision_time_hours: Optional[int] = None,
                    revision_time_days: Optional[int] = None,
                    revision_tz: str = 'UTC',
                    maturity_offset_hours: Optional[int] = None,
                    maturity_offset_days: Optional[int] = None,
                    maturity_time_hours: Optional[int] = None,
                    maturity_time_days: Optional[int] = None,
                    ) -> pd.Series:
    """
    Computes a series rebuilt from successive blocks of history, each
    linked to a distinct revision date. The revision dates are taken
    at regular time intervals determined by `revision_freq`,
    `revision_time` and `revision_tz`. The time lag between revision
    dates and value dates of each block is determined by
    `maturity_offset` and `maturity_time`.

    Example:

    `(block-staircase "forecast-series" #:revision_freq_days 1 #:revision_time_hours 11 #:maturity_offset_days 1)`

    """

    i = __interpreter__
    series = i.tsh.block_staircase(
        i.cn,
        name,
        __from_value_date__,
        __to_value_date__,
        revision_freq=build_argdict_delta(revision_freq_days, revision_freq_hours),
        revision_time=build_argdict(revision_time_days, revision_time_hours),
        revision_tz=revision_tz,
        maturity_offset=build_argdict_delta(maturity_offset_days, maturity_offset_hours),
        maturity_time=build_argdict(maturity_time_days, maturity_time_hours)
    )

    return series


@metadata('block-staircase')
def block_staircase_metadata(cn, tsh, tree):
    return {
        tree[1]: tsh.internal_metadata(cn, tree[1])
    }


@finder('block-staircase')
def block_staircase_finder(cn, tsh, tree):
    return {
        tree[1]: tree
    }


# holidays utils

def set_at_midnight(dt):
    return datetime(
        dt.year,
        dt.month,
        dt.day
    )


def compatible_date_at_midnight(tzaware, date):
    tzinfo = date.tzinfo
    date = set_at_midnight(date)
    if not tzaware:
        return date.replace(tzinfo=None)

    if tzaware and tzinfo is None:
        return date.replace(tzinfo=pytz.utc)

    return date


@func('holidays')
def get_holidays(__interpreter__,
                 __from_value_date__,
                 __to_value_date__,
                 country: str,
                 naive: Optional[bool]=False) -> pd.Series:
    """
    Compute a series whose values will be either 0 or 1 to signal the
    holydays.

    Takes a string for the 2-letters country code and an optional
    `naive` keyword (values #t or #f, #f by default) to force a naive
    series output.

    Example: `(holidays "fr")`

    """
    ctry = pycountry.countries.get(alpha_2=country.upper())
    hd = holidays.country_holidays(ctry.alpha_2)

    beginyear = datetime(datetime.now().year, 1, 1)
    endyear = datetime(datetime.now().year + 1, 1, 1)

    from_value_date = __from_value_date__ or beginyear
    to_value_date = __to_value_date__ or endyear
    from_value_date = set_at_midnight(from_value_date)
    to_value_date = set_at_midnight(to_value_date)

    if from_value_date:
        from_value_date = compatible_date_at_midnight(
            not naive,
            from_value_date
        )
    if to_value_date:
        to_value_date = compatible_date_at_midnight(
            not naive,
            to_value_date
        )

    index = pd.date_range(
        start=from_value_date,
        end=to_value_date,
    )
    if naive:
        index = index.tz_localize(None)
    elif index.dtype.tz is None:
        index = index.tz_localize('UTC')

    # another hack for another behviour of holidays:
    # their slicing is exclusionnary of the last value :/
    # it made kind of sense an won't be be corrected upstream
    to_value_date = to_value_date + timedelta(days=1)

    base = pd.Series([0.] * len(index), index)
    hol_index = pd.DatetimeIndex(
        hd[from_value_date:to_value_date]
    )
    if not naive:
        hol_index = hol_index.tz_localize('UTC')
    base[hol_index] = 1.
    return base


@metadata('holidays')
def holidays_metadata(cn, tsh, tree):
    args, kw = buildargs(tree[1:])
    naive = 'naive' in kw and kw.get('naive') or False
    if naive:
        return {
            'holidays': {
                'index_dtype': '<M8[ns]',
                'index_type': 'datetime64[ns]',
                'tzaware': False,
                'value_dtype': '<f8',
                'value_type': 'float64'
            }
        }
    country = args[0]
    tzones = pytz.country_timezones(country)
    assert len(tzones) == 1, f'Many available timezones for {country}, cannot compute'
    return {
        'holidays': {
            'tzaware': True,
            'index_type': 'datetime64[ns, UTC]',
            'value_type': 'float64',
            'index_dtype': '|M8[ns]',
            'value_dtype': '<f8'
        }
    }


@func('round')
def round(series: pd.Series,
          decimals: Optional[Number]=0) -> pd.Series:
    """
    Round element-wise considering the number of decimals specified (0 by default).

    Example: `(round (series "series-with-decimals") #:decimals 2)`
    """
    opts = series.options

    res = series.round(decimals)
    res.options = opts
    return res


@func('abs')
def abs(series: pd.Series) -> pd.Series:
    """
    Return the absolute value element-wise.

    Example: `(abs (series "series-with-negative-values"))`
    """
    opts = series.options

    res = series.abs()
    res.options = opts
    return res


@func('sub')
def sub(series1: pd.Series,
        series2: pd.Series) -> pd.Series:
    """
    Return the substraction of two series element-wise.

    Example: `(sub (series "series1") (series "series2"))`
    """
    df = _group_series(series1, series2)
    if not len(df):
        return empty_series(
            tzaware_serie(series1)
        )

    res = df['0'] - df['1']

    return res.dropna()
