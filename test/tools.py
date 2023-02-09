import pandas as pd


def tuples2series(series_as_tuples, index_name=None, name='indicator'):
    """Convert a list of (index, value) to a pandas Series"""
    idx, values = zip(*series_as_tuples)
    series = pd.Series(
        values,
        index=idx,
        name=name,
    )
    if index_name:
        series.index.name = index_name
    return series
