import pandas as pd
import numpy as np
import asyncio, ccxt
import ccxt.async_support as accxt
import Misso.services.utilities as utils
from sklearn.cluster import AgglomerativeClustering
from scipy.ndimage import maximum_filter1d, minimum_filter1d

import multiprocessing as mp
import time

class marketReviewer:
    @staticmethod
    def get_watch_list(from_file=True, path="storage/watch_list.json"):
        if from_file:
            watch_list = utils.load_from_json(file=path)
        else:
            watch_list = marketReviewer._get_watch_list()
        return watch_list

    @staticmethod
    def _get_watch_list(save_to_file=True, path="storage/watch_list.json"):
        exchange = ccxt.ftx()
        markets = exchange.load_markets()
        ls = []
        for sym, data in markets.items():
            if sym.endswith("USD:USD") and float(data["info"]["volumeUsd24h"]) > 1500000:
                ls.append(sym)
        if save_to_file:
            utils.save_to_json_dict(ls, file=path)
        return ls

    @staticmethod
    def normalize_tuple_list(a, cut_off=4):
        import numpy as np
        ar = np.array(a)
        _ar = np.vstack(ar[:,1]).astype(float)
        _ar = (_ar - _ar.mean()).flatten()
        if cut_off > 0:
            _ar[_ar > cut_off] = cut_off
            _ar[_ar < -cut_off] = -cut_off
        return ar[:, 0].tolist(), _ar.tolist()

    @staticmethod
    def normalize_rr_ranking(rr_ranking: dict):
        rr = {tx: {"buy":{}, "sell":{}} for tx in rr_ranking.keys()}
        for tx, sides in rr_ranking.items():
            for side, _rr in sides.items():
                k, v = marketReviewer.normalize_tuple_list(_rr)
                rr[tx][side] = dict(zip(k, v))
        return rr
    @staticmethod
    def _only_higher_timeframes(timeframes: list, tx: str):
        if tx != timeframes[0]:
            for i, td in enumerate(timeframes):
                if td == "tx":
                    return timeframes[i-1:]
        return timeframes

    @staticmethod
    def score_multi_timeframe(rr: dict, target_tx="1m", target_side="buy"):
        score = {k:0 for k in rr[target_tx][target_side].keys()}
        for tx in marketReviewer._only_higher_timeframes(list(rr.keys()), target_tx):
            for k in rr[target_tx][target_side].keys():
                if k not in rr[tx][target_side]:
                    continue
                score[k] += rr[tx][target_side][k]
        return score

    @staticmethod
    def add_both_to_rr(rr: dict, cut_off: float= 4):
        for tx in rr.keys():
            d = {}
            for s in rr[tx]["buy"].keys():
                both = rr[tx]["buy"][s] - rr[tx]["sell"][s]
                d[s] = both + cut_off if both < 0 else both - cut_off
            rr[tx]["both"] = d
        return rr

    @staticmethod
    def get_filtered_rr_ranking(market_reviews: dict, target_tx="1m", skip_buys=[], skip_sells=[], skip_both=[]):
        rr_dict = marketReviewer.sort_risk_reward(market_reviews)
        rr = marketReviewer.normalize_rr_ranking(rr_dict)
        rr = marketReviewer.add_both_to_rr(rr)
        sc_buy = marketReviewer.score_multi_timeframe(rr, target_tx, "buy")
        sc_sell = marketReviewer.score_multi_timeframe(rr, target_tx, "sell")
        sc_both = marketReviewer.score_multi_timeframe(rr, target_tx, "both")
        skip = skip_buys + skip_sells + skip_both
        sc_buy = {k:v for k, v in sorted(sc_buy.items(), key=lambda item: item[1], reverse=True) if v>0 and k not in skip}
        sc_sell = {k:v for k, v in sorted(sc_sell.items(), key=lambda item: item[1], reverse=True) if v>0 and k not in skip}
        sc_both = {k:v for k, v in sorted(sc_both.items(), key=lambda item: item[1], reverse=True) if v>0 and k not in skip_both}
        return sc_buy, sc_sell, sc_both

    @staticmethod
    def get_state(ref_market:str, ref_data: dict, timeframes: list=["4h", "1h", "15m", "1m"], market_maker: str="oldest"):
        return get_market_state(ref_market, ref_data, timeframes, market_maker)

    @staticmethod
    def fetch_states(markets: list, timeframes: list=["4h", "1h", "15m", "1m"], market_maker: str="oldest"):
        data = execute_asyncio(get_ohlcv_data_multi_timeframe, [None, markets], {"limit":500,"close":True, "timeframes":timeframes})
        return marketReviewer.get_states(data, timeframes, market_maker)

    @staticmethod
    def get_states(data: dict, timeframes: list=["4h", "1h", "15m", "1m"], market_maker: str="oldest", **kwargs):
        states = {}
        for ref_market, ref_data in data.items():
            states.update(get_market_state(ref_market, ref_data, market_maker=market_maker, timeframes=timeframes))
        return states

    @staticmethod
    def sort_risk_reward(states: dict, symbols_only=False):
        markets = list(states.keys())
        timeframes = list(states[markets[0]].keys())
        d = {}
        for t in timeframes:
            d[t] = {"buy":[], "sell":[]}
            for m in markets:
                if t in states[m]:
                    d[t]["buy"].append((m, states[m][t]["reward_risk"]["buy"]))
                    d[t]["sell"].append((m, states[m][t]["reward_risk"]["sell"]))
            d[t]["buy"] = sort_tuple_list(d[t]["buy"], keys_only=symbols_only)
            d[t]["sell"] = sort_tuple_list(d[t]["sell"], keys_only=symbols_only)
        return d

    @staticmethod
    def save_to_json(states: dict, file="market_states.json", dir="storage"):
        import Misso.services.helper as mh
        mh.save_to_json_dict(states, file=file, dir=dir)




# def get_markets_states(markets: list, timeframes: list=["4h", "1h", "15m", "1m"], market_maker: str="oldest", data: dict=None):
#     data = execute_asyncio(get_ohlcv_data_multi_timeframe, [None, markets, 500], {"close":True, "timeframes":timeframes}) if data is None else data
#     states = {}
#     for ref_market, ref_data in data.items():
#         states[ref_market] = get_market_state(ref_market, market_maker=market_maker, ref_data=ref_data, timeframes=timeframes)
#     return states

def _as_array(arr):
    if isinstance(arr, np.ndarray):
        return arr
    return np.array(arr)

def safe_merge_waves(lows, highs):
    min_dist = abs(lows[0] - lows[1])/2
    ls = np.sort(np.concatenate((lows, highs)))
    _ls = np.roll(ls, -1)
    return ls[np.logical_or(_ls - ls>min_dist, _ls - ls<0)]

def get_wave_target_pairs(market_state: dict, timeframe:str=None, buy_waves=None, sell_waves=None, by="next"):
    _market_state = safe_from_dict(market_state, timeframe, {})
    buy_waves = safe_from_dict(_market_state, {"data":"low_waves"}, buy_waves)
    sell_waves = safe_from_dict(_market_state, {"data":"high_waves"}, sell_waves)
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
    if timeframe:
        return {timeframe:wp}
    return wp


def sort_risk_reward(states: dict, symbols_only=True):
    markets = list(states.keys())
    timeframes = list(states[markets[0]].keys())
    d = {}
    for t in timeframes:
        d[t] = {"buy":[], "sell":[]}
        for m in markets:
            if t in states[m]:
                d[t]["buy"].append((m, states[m][t]["reward_risk"]["buy"]))
                d[t]["sell"].append((m, states[m][t]["reward_risk"]["sell"]))
        d[t]["buy"] = sort_tuple_list(d[t]["buy"], keys_only=symbols_only)
        d[t]["sell"] = sort_tuple_list(d[t]["sell"], keys_only=symbols_only)
    return d

def _get_market_state(args):
    return get_market_state(*args)

def get_market_state(ref_market, ref_data, timeframes: list=["4h", "1h", "15m", "1m"], market_maker: str="oldest"):
    try:
        _ref_data = ref_data[timeframes[-1]]
        if not isinstance(_ref_data, np.ndarray):
            _ref_data = np.array(_ref_data)
        ref_price = _ref_data[-1, 4]
    except:
        print(f"ERROR {ref_market} with ref_data_-keys: {ref_data.keys()} and ref_data {ref_data}")

    states = {}
    for tx, candles in ref_data.items():
        try:
            low_waves, high_waves = get_clustered_waves(candles, 40, 4)
        except:
            continue
        mr = [low_waves.min(), high_waves.max()]
        is_lower_mr = is_below_mid(mr, ref_price)
        cr = next_higher_lower_in_array(low_waves, ref_price) if is_lower_mr else next_higher_lower_in_array(high_waves, ref_price)
        if ref_price < cr[0]:
            cr[0] = next_lower_in_array(low_waves, ref_price)
        if ref_price > cr[1]:
            cr[1] = next_higher_in_array(high_waves, ref_price)
        is_lower_cr = is_below_mid(cr, ref_price)
        ll = next_lower_in_array(low_waves, ref_price)
        hh = next_higher_in_array(high_waves, ref_price)
        rr_sell, rr_buy = get_reward_risk_rations(cr, mr)
        waves = wave_dict(low_waves, high_waves)
        filtered_waves = filter_waves(waves)
        states[tx] = {
            "current_range":cr,
            "meta_range":mr,
            "next_lower_low":ll,
            "next_higher_high":hh,
            "states":{"is_lower_mr":bool(is_lower_mr),
                      "is_lower_cr":bool(is_lower_cr),
                      "has_broken_range10":has_broken_range(cr, candles, n=10),
                      "has_broken_range4":has_broken_range(cr, candles, n=4)},
            "data":{"low_waves":low_waves, "high_waves":high_waves},
            "waves":waves,
            "filtered_waves": filtered_waves,
            "reward_risk":{"sell":rr_sell, "buy":rr_buy}}
    return {ref_market:states}

def wave_dict(low_waves, high_waves):
    if not isinstance(low_waves, list):
        low_waves = list(low_waves)
    if not isinstance(high_waves, list):
        high_waves = list(high_waves)
    low_waves.reverse()
    d = {}
    for i, w in enumerate(low_waves):
        d[-(i+1)] = w

    for i, w in enumerate(high_waves):
        d[i+1] = w
    return d

def get_rel_range(range, i=0):
    if range[i] > 0:
        return abs(range[1] - range[0])/range[i]
    return 0

def get_reward_risk_rations(cr, mr):
    buy_pot = get_rel_range([cr[0], mr[1]], 0)
    buy_risk = get_rel_range([mr[0], cr[0]], 1)
    sell_pot = get_rel_range([mr[0], cr[1]], 1)
    sell_risk = get_rel_range([cr[1], mr[1]], 0)

    rr_buy = buy_pot/buy_risk if buy_risk > 0 else buy_pot/0.5
    rr_sell = sell_pot/sell_risk if sell_risk > 0 else sell_pot/0.5
    return rr_sell, rr_buy


def find_unrecovered_mm(df: pd.DataFrame, vol_factor: int=2, n: int=10, filter_type: str=None, return_last: bool=False):
    d = find_mm_candles(df, vol_factor, n)
    d = are_mm_neutralized(df, d)
    d = are_mm_triggered(df, d)

    mms = []
    for tx, v in d.items():
        if filter_type is not None and v["type"] != filter_type:
            continue  #skips element
        if not v["neutral"]:
            v["tx"] = tx
            mms.append(v)
    if len(mms) == 0:
        return None
    return mms[-1] if return_last else mms

def find_mm_candles(df, vol_factor=2, n=10, ):
    d = {}
    for i in df.index:
        if i < n:
            continue
        avg_volume = df.Volume.iloc[i-n:i].sum()/n
        if df.Volume.loc[i] > avg_volume*vol_factor:
            if df.Open.loc[i] > df.Close.loc[i]: #short
                d[df.Date.loc[i]] = {"type":"short", "high": df.High.loc[i], "low": df.Low.loc[i], "half": (df.High.loc[i] + df.Low.loc[i])/2, "target": df.High.loc[i],"triggered":False, "neutral":False}
            else:
                d[df.Date.loc[i]] = {"type":"long", "high": df.High.loc[i], "low": df.Low.loc[i], "half": (df.High.loc[i] + df.Low.loc[i])/2, "target": df.Low.loc[i],"triggered":False, "neutral":False}
    return d

def is_mm_neutralized(df, tx, v):
    if v["type"] == "short":
        if v["high"] <= df.High.loc[tx:].iloc[1:].max():
            return True
        return False
    else:
        if v["low"] >= df.Low.loc[tx:].iloc[1:].min():
            return True
        return False

def is_mm_triggered(df, tx, v):
    if v["type"] == "short":
        if v["half"] <= df.High.loc[tx:].iloc[1:].max():
            return True
        return False
    else:
        if v["half"] >= df.Low.loc[tx:].iloc[1:].min():
            return True
        return False

def are_mm_neutralized(df: pd.DataFrame, mm_candles: dict):
    _df = df.copy()
    _df = _df.set_index(_df.Date)

    for tx, v in mm_candles.items():
        mm_candles[tx]["neutral"] = is_mm_neutralized(_df, tx, v)
    return mm_candles

def are_mm_triggered(df: pd.DataFrame, mm_candles: dict, not_neutral_only: bool=True):
    _df = df.copy()
    _df = _df.set_index(_df.Date)

    for tx, v in mm_candles.items():
        if not_neutral_only and v["neutral"]:
            continue
        mm_candles[tx]["triggered"] = is_mm_triggered(_df, tx, v)
    return mm_candles


## State conditions
def is_below_mid(range: list, ref: float):
    mid = abs(range[0] + range[1])/2
    return ref < mid

def has_broken_range(range: list, candles: np.ndarray, n: int=10):
    low = range[0] > candles[-n:,3].min()
    high = range[1] < candles[-n:,2].max()
    return [low, high]

## background methods

def get_cluster(candles, wave_length, num_clusters, target="min"):
    def rolling_min(a, W, fillna=np.nan):
        out_dtype = np.full(0,fillna).dtype
        hW = (W-1)//2 # Half window size
        out = minimum_filter1d(a,size=W, origin=hW)
        if out.dtype is out_dtype:
            out[:W-1] = fillna
        else:
            out = np.concatenate((np.full(W-1,fillna), out[W-1:]))
        return out

    def rolling_max(a, W, fillna=np.nan):
        out_dtype = np.full(0,fillna).dtype
        hW = (W-1)//2 # Half window size
        out = maximum_filter1d(a,size=W, origin=hW)
        if out.dtype is out_dtype:
            out[:W-1] = fillna
        else:
            out = np.concatenate((np.full(W-1,fillna), out[W-1:]))
        return out

    def get_min_waves(ca, wave_length):
        y = rolling_min(ca[:,3], wave_length)
        idxs = np.unique(y, return_index=True)[1]
        y = np.array([y[idx] for idx in sorted(idxs)])
        y = y[~np.isnan(y)]
        y = np.column_stack((y, np.full(len(y), 1)))
        return y

    def get_max_waves(ca, wave_length):
        y = rolling_max(ca[:,2], wave_length)
        idxs = np.unique(y, return_index=True)[1]
        y = np.array([y[idx] for idx in sorted(idxs)])
        y = y[~np.isnan(y)]
        y = np.column_stack((y, np.full(len(y), 1)))
        return y

    if not isinstance(candles, np.ndarray):
        candles = np.array(candles)

    if target == "min":
        x = get_min_waves(candles, wave_length)
    else:
        x = get_max_waves(candles, wave_length)

    # Find Support/Resistance with clustering using the rolling stats
    # Initialize Agglomerative Clustering
    cluster = AgglomerativeClustering(n_clusters=num_clusters, affinity='euclidean', linkage='ward')
    cluster.fit_predict(x)

    waves = np.column_stack((x, cluster.labels_))
    _cluster = np.unique(cluster.labels_)
    if target == "min":
        return np.sort(np.array([np.min(waves[np.where(waves[:,2] == cl)][:,0]) for cl in _cluster]))
    else:
        return np.sort(np.array([np.max(waves[np.where(waves[:,2] == cl)][:,0]) for cl in _cluster]))

def get_clustered_waves(candles, wave_length, num_clusters):
    min_waves = get_cluster(candles, wave_length, num_clusters, target="min")
    max_waves = get_cluster(candles, wave_length, num_clusters, target="max")
    return min_waves, max_waves

### HELPER

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

def next_lower_in_array(array, key, treshhold=0.0):
    a = -np.sort(-array)
    key = key*(1-treshhold)
    try:
        return a[(a < key).argmax()]
    except:
        return None

def candles_to_df(candles, columns=["Date", "Open","High","Low","Close","Volume"]):
    df = pd.DataFrame(candles, columns=columns)
    df[columns[0]] = pd.to_datetime(df[columns[0]], unit="ms")
    return df

def safe_format_since(since):
    if isinstance(since, str):
        since = str_to_timestamp(since) * 1000
    return since

def str_to_timestamp(string):
    return int(pd.to_datetime(string).timestamp())

def sort_tuple_list(tuple_list: list, key_index: int=1, descending: bool=True, keys_only: bool=False):
    def takeKey(elem):
        return elem[key_index]
    if descending:
        tuple_list.sort(key=takeKey, reverse=True)
    else:
        tuple_list.sort(key=takeKey)
    return [key for key, _ in tuple_list] if keys_only else tuple_list

def safe_from_dict(d: dict, key:str, default: any=None):
    if isinstance(key, dict):
        for k1, k2 in key.items():
            if k1 in d and k2 in d[k1]:
                return d[k1][k2]
            else:
                return default
    if key in d:
        return d[key]
    else:
        return default
## ASYNC methods

async def get_ohlcv_data_multi_timeframe(a_exchange, watch_list, limit=500, close=False, since=None, timeframes=["1m", "15m", "1h", "4h"]):
    if a_exchange is None:
        a_exchange = accxt.ftx()
    if isinstance(watch_list, str):
        watch_list = [watch_list]
    symbol_timeframes = [(s, t) for s in watch_list for t in timeframes]
    if since is not None:
        since = safe_format_since(since)
    tasks = [_get_ohlcv_data_multi_timeframe(a_exchange, sym, tx, limit, since=since) for sym, tx in symbol_timeframes]
    data = await asyncio.gather(*tasks, return_exceptions=True)

    candles = {}
    failed = False
    for d in data:
        if not isinstance(d, dict):
            print(d)
            failed = True
            continue
        for k, v in d.items():
            if not k in candles:
                candles[k] = {}
            candles[k][v["timeframe"]] = v["data"]
    if failed:
        print(data)
    if close:
        await a_exchange.close()
    return candles

async def _get_ohlcv_data_multi_timeframe(a_exchange: accxt, symbol: str, timeframe: list, limit: int=500, since: int=None):
    resp = await a_exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit, since=since)
    return {symbol:{"timeframe":timeframe, "data":np.array(resp)}}

def execute_asyncio(method, args, kwargs):
    import asyncio
    from Misso.services.async_helper import RunThread
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # 'RuntimeError: There is no current event loop...'
        loop = None
    if loop is not None and loop.is_running():
        thread = RunThread(method, args, kwargs)
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.run(method(*args, **kwargs))

def filter_waves(waves: dict):
    """remove -1 and 1 wave from wave_dict"""
    return {k: v for k, v in waves.items() if k not in [-1,1]}


def filter_waves_tx(review):
    r = {}
    for tx, data in review.items():
        r[tx] = filter_waves(data["waves"])
    return r

def merge_tx_waves(filtered_waves: dict):
    def is_in_range(x, mi, mx):
        if mi is None or mx is None:
            return False
        return x >= mi and x <=mx

    waves = []
    mi, mx = None, None

    for tx, wa in filtered_waves.items():
        for w in wa.values():
            if not is_in_range(w, mi, mx):
                waves.append(w)
        mi = min(waves)
        mx = max(waves)

    waves.sort()
    return waves

if __name__ == '__main__':
    from pprint import pp
    import tracemalloc

    tracemalloc.start()
    pp(get_market_state())
    print(tracemalloc.get_traced_memory())
    tracemalloc.stop()