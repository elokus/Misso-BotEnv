import json, ccxt, os, time
import Misso.services.cust_decorator as decor
import numpy as np
import pandas as pd


############################
###    FILE/OS UTILITIES
### ------------------------


def get_latest_json(prefix: str, dir: str):
    files = [f for f in os.listdir(dir) if f.startswith(prefix)]
    if len(files) == 0:
        return None
    files.sort()
    with open(f"{dir}/{files[-1]}", "r") as f:
        data = json.load(f)
    return data


def save_to_json_dict(state_dict, file="state_dict.json", dir=None):
    import json
    import os
    import datetime
    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.bool_):
                return bool(obj)
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, pd.Timestamp):
                return obj.strftime('%Y-%m-%d %X')
            if isinstance(obj, datetime.datetime):
                return obj.strftime('%Y-%m-%d %X')
            return json.JSONEncoder.default(self, obj)
    if not dir is None:
        file = os.path.join(dir, file)
    with open(file, 'w') as fp:
        json.dump(state_dict, fp, cls=NpEncoder)
    return file

def load_from_json(file):
    import json

    with open(file, "r") as f:
        _dict = json.load(f)
    return _dict


def get_path_prefix(main="Misso"):
    p = os.getcwd()
    i = ""
    while os.path.split(p)[1] != main:
        i += "..\\"
        p = os.path.split(p)[0]
    return i, p

def is_running_in_jupyter():
    import sys
    return "ipykernel_launcher.py" in sys.argv[0]

def parent_to_path():
    import os, sys, inspect
    currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
    parentdir = os.path.dirname(currentdir)
    sys.path.insert(0, parentdir)

##################################
###    TRANSFORM/AGGREGATION UTILITIES
### ------------------------------

def last_timestamp_candles(candles: np.ndarray):
    """ get last unix timestamp from ohlcv data array """
    if not isinstance(candles, np.ndarray):
        candles = np.array(candles)
    return int(candles[-1,0]/1000)

def sort_tuple_list(tuple_list: list, key_index: int=1, descending: bool=True, keys_only: bool=False):
    def takeKey(elem):
        return elem[key_index]
    if descending:
        tuple_list.sort(key=takeKey, reverse=True)
    else:
        tuple_list.sort(key=takeKey)
    return [key for key, _ in tuple_list] if keys_only else tuple_list

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


def _as_array(arr):
    if isinstance(arr, np.ndarray):
        return arr
    return np.array(arr)


def ar_by_col(ar: np.array, col: str):
    d = {'Date': 0, 'Open': 1, 'High': 2, 'Low': 3, 'Close': 4, 'Volume': 5}
    return ar[:,d[col]]

def next_higher_in_arrays(array_1, array_2, key, treshhold=0.03):
    next_1 = next_higher_in_array(array_1, key, treshhold)
    next_2 = next_higher_in_array(array_2, key, treshhold)
    if next_2 is None:
        return next_1
    elif next_1 is None:
        return next_2
    elif next_1 > next_2 and next_2 > key:
        return next_2
    return next_1

def next_lower_in_arrays(array_1, array_2, key, treshhold=0.03):
    next_1 = next_lower_in_array(array_1, key, treshhold)
    next_2 = next_lower_in_array(array_2, key, treshhold)
    if next_2 is None:
        return next_1
    elif next_1 is None:
        return next_2
    elif next_1 < next_2 and next_2 < key:
        return next_2
    return next_1

def next_higher_lower_in_array(array, key):
    if isinstance(array, list) or isinstance(array, pd.Series):
        array = np.array(array)
    higher = next_higher_in_array(array, key, treshhold=0.0)
    lower = next_lower_in_array(array, key, treshhold=0.0)
    return [lower, higher]

def next_higher_in_array(array, key, treshhold=0.0):
    a = np.sort(array)
    key = key*(1+treshhold)
    try:
        return a[(a > key).argmax()]
    except:
        return None


def next_lower_in_array(array, key, treshhold=0.03):
    a = -np.sort(-array)
    key = key*(1-treshhold)
    try:
        return a[(a < key).argmax()]
    except:
        return None

##################################
###    MARKET UTILITIES
### ------------------------------


def fetch_save_market_states(suffix: str="00", watch_list_from_file: bool=True, return_path: bool=True):
    """Fetch market review via marketReviewer class using current watch_list and save to json file

    :param suffix: string suffix for filename e.g. run_id, version, date
    :param watch_list_from_file: bool if True loading watch_list from storage/watch_list.json
                                        else loading watch_list from exchange
    :param return_path: bool (default True)
    :returns: market_review dict (optional: return market_review and path to file)
    """
    from Misso.models.streamer.reviewer import marketReviewer
    ms = marketReviewer.get_states(get_watch_list(from_file=watch_list_from_file))
    path = save_to_json_dict(ms, file=f"market_states_{suffix}.json", dir="storage")
    if return_path:
        return ms, path
    return ms


def get_watch_list(from_file: bool=True, path: str="storage/watch_list.json"):
    """Get current watch list from storage file or load from ccxt exchange

    :param from_file: bool if True loading from file located in storage/ (default is True)
    :param path: str path to storage file only used if from file is True
    :returns: a list of market symbols (future symbols in ccxt format)
    """
    if from_file:
        watch_list = load_from_json(file=path)
    else:
        watch_list = _get_watch_list()
    return watch_list


def _get_watch_list(save_to_file: bool=True, path: str="storage/watch_list.json", min_vol: float=1500000):
    """Get current watch list by load from ccxt exchange

    :param save_to_file: bool if True saving json file to 'path' (default is True)
    :param path: str path to storage file only used if save_to_file is True
    :param min_vol: minimum 24h volume for filtering symbol list (default is 1.5 Mio USD)
    :returns: a list of market symbols (future symbols in ccxt format)
    """
    exchange = ccxt.ftx()
    markets = exchange.load_markets()
    ls = []
    for sym, data in markets.items():
        if sym.endswith("USD:USD") and float(data["info"]["volumeUsd24h"]) > min_vol:
            ls.append(sym)
    if save_to_file:
        save_to_json_dict(ls, file=path)
    return ls


##################################
###    EXCHANGE DATA UTILITIES
### ------------------------------

def change_sub(exchange, subaccount):
    from Misso.config import API_CONFIG
    exchange = exchange or initialize_exchange_driver("Main")
    if subaccount in API_CONFIG:
        exchange.headers["FTX-SUBACCOUNT"] = subaccount
        return exchange
    else:
        print("subaccount not in exchange_config.yaml")

def get_subaccounts():
    from Misso.config import API_CONFIG
    return list(API_CONFIG.keys())

def get_current_subaccount(exchange: ccxt):
    return exchange.headers["FTX-SUBACCOUNT"]

def get_exchange_config_for_subbaccount(subaccount):
    from Misso.config import API_CONFIG
    return API_CONFIG[subaccount]

def init_exchange_driver_from_config(config=None):
    import ccxt
    ftx = ccxt.ftx()
    sub = config["subaccount"]
    ftx.apiKey = config["api_key"]
    ftx.secret = config["api_secret"]
    if sub != "Main":
        ftx.headers = {"FTX-SUBACCOUNT":sub}
    ftx.load_markets()
    return ftx

def init_exchange_driver_from_config_async(config=None):
    import ccxt.async_support as ccxt
    ftx = ccxt.ftx()
    sub = config["subaccount"]
    ftx.apiKey = config["api_key"]
    ftx.secret = config["api_secret"]
    ftx.headers = {"FTX-SUBACCOUNT":sub}
    return ftx

async def close_exchange(exchange):
    await exchange.close()

def initialize_exchange_driver(subaccount, init_async=False, exchange=None):
    import asyncio
    if exchange is not None:
        asyncio.run(close_exchange(exchange))
    config = get_exchange_config_for_subbaccount(subaccount)
    if init_async:
        return init_exchange_driver_from_config_async(config)
    else:
        return init_exchange_driver_from_config(config)

def safe_last_price(exchange, symbol, last=None):
    if not last:
        try:
            last = exchange.fetch_ticker(symbol)["last"]
        except:
            time.sleep(0.2)
            _exchange = ccxt.ftx()
            last = _exchange.fetch_ticker(symbol)["last"]
    return last

def price_in_range(price: float, range: list, buffer: float=0):
    delta = abs(range[0] - range[1]) * buffer
    return price >= range[0]-delta and price <= range[1]+delta

def safe_parse_side(response):
    parse_side = dict(long="buy", short="sell", buy="buy", sell="sell")
    return parse_side[response["side"]]

def safe_parse_exit_side(response):
    parse_side = dict(long="sell", short="buy", buy="sell", sell="buy")
    return parse_side[response["side"]]

def safe_parse_break_even(response):
    try:
        break_even = float(response["info"]["recentBreakEvenPrice"])
    except:
        break_even = response["entryPrice"]
    return break_even

def safe_value_to_size(exchange, symbol, value, minProvideSize=True):
    m = exchange.market(symbol)
    min_size = float(m["info"]["minProvideSize"]) if minProvideSize else m["limits"]["amount"]["min"]
    s = value/float(m["info"]["last"])
    size = s if s > min_size else min_size
    size = float(exchange.amount_to_precision(symbol, size))
    return size

def is_stop_order(oid: str):
    return float(oid) < 10**10

@decor.safe_request
def safe_cancel_order(exchange: ccxt, oid: str):
    if oid is None:
        return
    elif is_stop_order(oid):
        return exchange.cancel_order(oid, params={"type":"stop"})
    else:
        return exchange.cancel_order(oid)

@decor.safe_request
def market_close_position(exchange, symbol, side, size):
    return exchange.create_market_order(symbol, side, size)


##################################
###    WAVE STRATEGY UTILITIES
### ------------------------------

def get_order_size_factor(tx0, tx1, default=2):
    d = {"1m":1, "15m":4, "1h":8, "4h":12}
    if tx0 in d and tx1 in d:
        return d[tx1]/d[tx0]
    return default

def safe_merge_waves(lows, highs):
    min_dist = abs(lows[0] - lows[1])/2
    ls = np.sort(np.concatenate((lows, highs)))
    _ls = np.roll(ls, -1)
    return ls[np.logical_or(_ls - ls>min_dist, _ls - ls<0)]

def get_wave_pairs(waves: dict, target_by: str, filtered_waves: dict):
    min_dist = abs(waves[1] - waves[2])/waves[1]/2
    buy_ids = [i for i in waves.keys() if i < 0]
    buy_ids.sort()
    sell_ids = [i for i in waves.keys() if i > 0]
    sell_ids.sort()
    assert len(buy_ids) == len(sell_ids), "different length buy/sell waves"
    index_dict = dict(zip(buy_ids, sell_ids))
    index_dict.update(dict(zip(sell_ids, buy_ids)))
    waves_array = np.array(list(filtered_waves.values()))
    wp = {}
    for idx, wave in waves.items():
        if target_by == "index":
            wp[idx] = (wave, waves[index_dict[idx]])
        if target_by == "next":
            if idx < 0:
                wp[idx] = (wave, next_higher_in_array(waves_array, wave, treshhold=min_dist))
            else:
                wp[idx] = (wave, next_lower_in_array(waves_array, wave, treshhold=min_dist))
    return wp

def scale_wave_pairs(wp: dict, target_factor: float):
    _wp = {}
    for idx, wps in wp.items():
        if idx < 0: #buy
            entry = wps[0]
            exit = wps[0] + target_factor* abs(wps[0] - wps[1])
        else:
            entry = wps[0]
            exit = wps[0] - target_factor* abs(wps[0] - wps[1])
        _wp[idx] = (entry, exit)
    return _wp

def get_wave_target_pairs(buy_waves=None, sell_waves=None, by="next"):
    wp = {"buy":{}, "sell":{}}
    if not buy_waves is None and not sell_waves is None:
        merged_waves = safe_merge_waves(buy_waves, sell_waves)
        min_dist = (abs(merged_waves[0] - merged_waves[1])/merged_waves[0])/2
        buy_waves = -np.sort(-_as_array(buy_waves))
        _w = -np.sort(-_as_array(sell_waves))
        for i, w in enumerate(buy_waves):
            if by == "index":
                wp["buy"][i] = (w, _w[i])
            else:
                wp["buy"][i] = (w, next_higher_in_array(merged_waves, w, treshhold=min_dist))
        sell_waves = np.sort(_as_array(sell_waves))
        _w = np.sort(_as_array(buy_waves))
        for i, w in enumerate(sell_waves):
            if by == "index":
                wp["sell"][i] = (w, _w[i])
            else:
                wp["sell"][i] = (w, next_lower_in_array(merged_waves, w, treshhold=min_dist))
    return wp

def is_valid_wave_pair(slot: str, last: float, idx: int, wp: tuple):
    if slot == "buy":
        if idx < 0:
            return wp[0] <= last
    if slot == "sell":
        if idx > 0:
            return wp[0] >= last
    if slot == "both":
        return wp[0] <= last if idx < 0 else wp[0] >= last

##################################
###    BASE CLASSES
### ------------------------------

class BaseDC(object):
    def __post_init__(self):
        pass