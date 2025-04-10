
import typing

import pytz
import pycountry


# import pandas as pd
# import pprint as pp
# df = pd.read_html('https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html')[2]
# result = []
# for val in df['Frequency String'].to_list():
#     if pd.isnull(val):
#         continue
#     val = val.split(' ')[0]
#     result.append(val.replace("""'""", """"""))
# pp.pprint(result)

PERIOD_OFFSETS = typing.Literal[
    'B',
    'C',
    'W',
    'WOM',
    'LWOM',
    'ME',
    'MS',
    'BME',
    'BMS',
    'CBME',
    'CBMS',
    'SME',
    'SMS',
    'QE',
    'QS',
    'BQE',
    'BQS',
    'REQ',
    'YE',
    'YS',
    'BYE',
    'BYS',
    'RE',
    'bh',
    'cbh',
    'd',
    'D',
    'h',
    'min',
    's',
    'ms',
    'us',
    'ns'
]


CALCULATION_METHODS = typing.Literal[
    'asfreq',
    'bfill',
    'count',
    'ffill',
    'first',
    'interpolate',
    'last',
    'max',
    'mean',
    'median',
    'min',
    'nearest',
    'sem',
    'size',
    'std',
    'sum',
    'var',
]


FILL_METHODS = typing.Literal[
    'ffill',
    'bfill',
    'ffill, bfill',
    'bfill, ffill',
]


LEAP_DAY_RULES = typing.Literal[
    'as_is',
    'ignore',
    'linear',
]

# pytype: disable=invalid-annotation

TIMEZONES = typing.Literal[
    *(pytz.all_timezones)
]


BY_VALUE_OPS = typing.Literal[
    '<',
    '<=',
    '>',
    '>=',
]


HOLIDAYS_COUNTRIES = typing.Literal[
    *(country.alpha_2.lower() for country in pycountry.countries)
]
