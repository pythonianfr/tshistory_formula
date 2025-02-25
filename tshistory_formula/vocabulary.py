
import typing

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
    'A',
    'AS',
    'B',
    'C',
    'W',
    'WOM',
    'LWOM',
    'M',
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
    'H',
    'min',
    's',
    'ms',
    'us',
    'ns',
    'T'
]


CALCULATION_METHODS = typing.Literal[
    'mean',
    'min',
    'max',
    'median',
    'sum',
    'interpolate',
    'count',
    'ffill',
    'last'
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


TIMEZONES = typing.Literal[
    'UTC',
    'CET',
    'EST',
    'GMT',
    'ETC/GMT+3',
    'Europe/Paris',
    'Europe/London',
    'Asia/Tokyo',
    'Europe/Berlin',
    'Europe/Madrid',
    'Europe/Rome',
    'Europe/Zurich',
    'Ameria/New_York',
    'Ameria/Los_Angeles',
    'America/Blanc-Sablon',
    'America/Coral_Harbour',
    'America/Halifax',
    'America/Regina',
    'America/St_Johns',
    'America/Vancouver',
    'America/Toronto',
    'America/Whitehorse',
    'America/Winnipeg',
    'America/Yellowknife',
    'US/Central',
    'US/Eastern',
    'US/Pacific',
    'utc'
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
