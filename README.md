TSHISTORY FORMULA
=================

# Purpose

This [tshistory][tshistory] component provides a formula language to
build computed series.

Formulas are defined using a simple lisp-like syntax, using a
pre-defined function library.

Formulas are read-only series (you can't `update` or `replace`
values).

They also have an history, which is built, time stamps wise, using the
union of all constituent time stamps, and value wise, by applying the
formula.

Because of this the `staircase` operator is available on formulae.
Some `staircase` operations can have a very fast implementation if the
formula obeys commutativity rules.

[tshistory]: https://hg.sr.ht/~pythonian/tshistory


# Formula

## General Syntax

Formulas are expressed in a lisp-like syntax using `operators`,
positional (mandatory) parameters and keyword (optional) parameters.

The general form is:

 `(<operator> <param1> ... <paramN> #:<keyword1> <value1> ... #:<keywordN> <valueN>)`

Here are a couple examples:

* `(add (series "wallonie") (series "bruxelles") (series "flandres"))`

Here we see the two fundamental `add` and `series` operators at work.

This would form a new synthetic series out of three base series (which
can be either raw series or formulas themselves).

Some notes:

* operator names can contain dashes or arbitrary caracters

* literal values can be: `3` (integer), `5.2` (float), `"hello"`
  (string) and `#t` or `#f` (true ot false)


## Pre-defined operators

### *

Performs a scalar product on a series.

Example: `(* -1 (series "positive-things"))`

### +

Add a constant quantity to a series.

Example: `(+ 42 (series "i-feel-undervalued"))`

### /

Perform a scalar division between numbers or a series and a scalar.

Example: `(/ (series "div-me") (/ 3 2))`

### add

Linear combination of two or more series. Takes a variable number
of series as input.

Example: `(add (series "wallonie") (series "bruxelles") (series "flandres"))`

To specify the behaviour of the `add` operation in the face of missing
data, the series can be built with the `fill` keyword. This option is
only really applied when several series are combined. By default, if
an input series has missing values for a given time stamp, the
resulting series has no value for this timestamp (unless a fill rule
is provided).

### clip

Set an upper/lower threashold for a series. Takes a series as
positional parameter and accepts two optional keywords `min` and `max`
which must be numbers (integers or floats).

Example: `(clip (series "must-be-positive") #:min 0)`

### date

Produces an utc timestamp from its input string date in iso format.

The `tz` keyword allows to specify an alternate time zone.
The `naive` keyword forces production of a naive timestamp.
Both `tz` and `naive` keywords are mutually exlcusive.

### div

Element wise division of two series.

Example: `(div (series "$-to-€") (series "€-to-£"))`

### min

Computes the row-wise minimum of its input series.

Example: `(min (series "station0") (series "station1") (series "station2"))`

### max

Computes the row-wise maximum of its input series.

Example: `(max (series "station0") (series "station1") (series "station2"))`

### mul

Element wise multiplication of series. Takes a variable number of series
as input.

Example: `(mul (series "banana-spot-price ($)") (series "$-to-€" #:fill 'ffill'))`

This might convert a series priced in dollars to a series priced in
euros, using a currency exchange rate series with a forward-fill
option.

### naive

Allow demoting a series from a tz-aware index (strongly recommended)
to a tz-naive index (unfortunately sometimes unavoidable for interop
with other tz-naive series).

One must provide a country code and a target timezone.

Example: `(naive (series "tz-aware-series-from-poland") "PL" "Europe/Warsaw")`

### priority

The priority operator combines its input series as layers. For each
timestamp in the union of all series time stamps, the value comes from
the first series that provides a value.

Example: `(priority (series "realized") (series "nominated") (series "forecasted"))`

Here `realized` values show up first, and any missing values come from
`nominated` first and then only from `forecasted`.

### resample

Resamples its input series using `freq` and the aggregation method
`method` (as described in the pandas documentation).

Example: `(resample (series "hourly") "D")`

### row-mean

This operator computes the row-wise mean of its input series using the
series `weight` option if present. The missing points are handled as
if the whole series were absent.

Example: `(row-mean (series "station0") (series "station1" #:weight 2) (series "station2"))`

Weights are provided as a keyword to `series`. No weight is
interpreted as 1.

### series

The `series` operator accepts several keywords:

* `fill` to specify a filling policy to avoid `nans` when the series
  will be `add`ed with others; accepted values are `"ffill"`
  (forward-fill), `"bfill"` (backward-fill) or any floating value.

For instance in `(add (series "a" #:fill 0) (series "b")` will make
sure that series `a`, if shorter than series `b` will get zeroes
instead of nans where `b` provides values.


### slice

This allows cutting a series at date points. It takes one positional
parameter (the series) and two optional keywords `fromdate` and
`todate` which must be strings in the [iso8601][iso8601] format.

Example: `(slice (series "cut-me") #:fromdate "2018-01-01")`

[iso8601]: https://en.wikipedia.org/wiki/ISO_8601

### std

Computes the standard deviation over its input series.

Example: `(std (series "station0") (series "station1") (series "station2"))`

### timedelta

Takes a timestamp and a number of years, months, weekds, days,
hours, minutes (int) and computes a new date according to the asked
delta elements.

Example: `(timedelta (date "2020-1-1") #:weeks 1 #:hours 2)`

### today

Produces a timezone-aware timestamp as of today

The `tz` keyword allows to specify an alternate time zone.
The `naive` keyword forces production of a naive timestamp.
Both `tz` and `naive` keywords are mutually exlcusive.

Example: `(today)`


# Series API

A few api calls are added to the `tshistory` base:

* `.register_formula` to define a formula

* `.eval_formula` to evaluate on-the-fly a formula (useful to check
  that it computes before registering it)

## register_formula

Exemple:

```python
  tsa.register_formula(
      'my-sweet-formula',
      '(* 3.14 (series "going-round"))'
  )
```

# eval_formula

Example:

```python
 >>> tsa.eval_formula('(* 3.14 (series "going-round"))')
 ...
 2020-01-01    3.14
 2020-01-02    6.28
 2020-01-03    9.42
 dtype: float64
```
