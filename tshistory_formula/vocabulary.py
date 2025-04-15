
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
    # 'C',
    'W',
    #'WOM', -> "Prefix \'WOM\' requires a suffix."
    #'LWOM',
    'ME',
    'MS',
    'BME', # Business month end
    'BMS', # Business month start
    # 'CBME',
    # 'CBMS',
    'SME', # every 15th and last day of the month
    'SMS', # every first day and 15th of the month
    'QE',
    'QS',
    'BQE',
    'BQS',
    # 'REQ', REQ, failed to parse with error message: TypeError('_parse_suffix() takes exactly 3 positional arguments (0 given)')"
    'YE',
    'YS',
    'BYE',
    'BYS',
    #'RE', RE, failed to parse with error message: TypeError('_parse_suffix() takes exactly 3 positional arguments (0 given)')")
    'bh', # business hours (from 9 to 16 every business day)
    # 'cbh',
    'D',
    'h',
    'min',
    's',
    # 'ms',
    # 'us',
    # 'ns'
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
