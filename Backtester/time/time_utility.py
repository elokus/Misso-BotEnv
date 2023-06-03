import numpy as np
import pandas as pd


### NEW:
MIN_TIMESTAMP = 157783680000   #2020-01-01

def NOW():
    return get_timestamp()

def tx_in_ms(timeframe: str) -> int:
    if timeframe.endswith("s"):
        t = int(timeframe[:-1])
    elif timeframe.endswith("m"):
        t = int(timeframe[:-1])*60
    elif timeframe.endswith("s"):
        t = int(timeframe[:-1])
    elif timeframe.endswith("h"):
        t = int(timeframe[:-1])*60*60
    elif timeframe.endswith("d") or timeframe.endswith("D"):
        t = int(timeframe[:-1])*60*60*24
    elif timeframe.endswith("w") or timeframe.endswith("W"):
        t = int(timeframe[:-1])*60*60*24*7
    elif timeframe.endswith("M"):
        t = int(timeframe[:-1])*60*60*24*30
    else:
        return None
    return int(t*1000) if t > 0 else 1

def offset_tms(tms: int | str, tx: str, offset: int) -> int:
    _tms = safe_tms(tms)
    return int(_tms + offset * tx_in_ms(tx))

### Work in Progress:

def random_timestamp(start_date: str, end_date: str):
    """string date range to random timestamp in ms"""
    from random import randint
    return int(randint(str_to_timestamp(start_date), str_to_timestamp(end_date)) * 1000)


def get_timestamp():
    """TODO: dynamically route time source EXCHANGE or Backtest based on env"""
    import time
    return int(time.time()*1000)

def round_timestamp(tms: int, timeframe: str) -> int:
    x = tx_in_ms(timeframe)
    return (tms // x) * x

def to_tms(date: str):
    return int(pd.to_datetime(date).timestamp() * 1000)

def safe_tms(date: str | int | float) -> int:
    """Returns a timestamp in milliseconds regardless of the input Format"""
    match date:
        case str():
            return to_tms(date)
        case int() if date < 100000000000:
            return date * 1000
        case int():
            return date
        case float():
            return safe_tms(int(date))
        case _:
            raise ValueError("date format error")

def count_tms(start: str | int, end: str | int, timeframe: str = "1m") -> int:
    return (safe_tms(end) - safe_tms(start)) // tx_in_ms(timeframe)

def tms_to_str(tms: int) -> str:
    return str(pd.to_datetime(tms, unit="ms"))

### OLD:

def last_timestamp_candles(candles: np.ndarray):
    """ get last unix timestamp from ohlcv data array """
    if not isinstance(candles, np.ndarray):
        candles = np.array(candles)
    return int(candles[-1,0]/1000)

def timeframe_string_to_seconds(timeframe):
    if timeframe.endswith("s"):
        t = int(timeframe[:-1])
    elif timeframe.endswith("m"):
        t = int(timeframe[:-1])*60
    elif timeframe.endswith("s"):
        t = int(timeframe[:-1])
    elif timeframe.endswith("h"):
        t = int(timeframe[:-1])*60*60
    elif timeframe.endswith("d"):
        t = int(timeframe[:-1])*60*60*24
    return t

def timeframe_string_to_unit(timeframe, unit="minutes"):
    if unit == "seconds":
        return timeframe_string_to_seconds(timeframe)
    if unit == "minutes":
        return timeframe_string_to_seconds(timeframe)/60
    if unit == "hours":
        return timeframe_string_to_seconds(timeframe)/60/60
    if unit == "days":
        return timeframe_string_to_seconds(timeframe)/60/60/24

def enum_tx_dict(timeframes=["1m", "15m", "1h", "4h"]):
    return {i:tx for i, tx in enumerate(timeframes)}

def tx_enum_dict(timeframes):
    return {tx:i for i, tx in enumerate(timeframes)}

def index_by_timeframe(timeframe, timeframes=["1m", "15m", "1h", "4h"]):
    return tx_enum_dict(timeframes)[timeframe]

def timeframe_by_index(index, timeframes=["1m", "15m", "1h", "4h"]):
    return enum_tx_dict(timeframes)[index]

def safe_format_since(since):
    if isinstance(since, str):
        since = str_to_timestamp(since) * 1000
    return since

def str_to_timestamp(string):
    return int(pd.to_datetime(string).timestamp())

def timestamp_to_str(timestamp: int):
    return pd.to_datetime(timestamp, unit="s")