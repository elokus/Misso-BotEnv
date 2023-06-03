from Backtester.database.helper import query_order_pair, query_drawdown
import pandas as pd


def group_targets_to_dict(ref: float, levels: dict, a: float = 1.0, dec: int=2) -> dict:

    def adj_dist(entry, exit) -> tuple:
        """takes entry, exit pair and adjust distance between them with factor a
        returns: entry, adj. exit"""
        return entry, round(entry + (exit - entry) * a, dec)

    keys, level = list(levels.keys()), list(levels.values())
    # keys (tx, txi)
    buys = {("buy", *keys[i]): adj_dist(e, level[i+1]) for i, e in enumerate(level[:-1]) if e < ref}
    sells = {("sell", *keys[i+1]): adj_dist(e, level[i]) for i, e in enumerate(level[1:]) if e > ref}
    return {**buys, **sells}

def batch_adjustments(grouped_dict: dict, cond_adj: dict = None, dec: int = 2) -> dict:
    if cond_adj:
        for key, factor in cond_adj.items():
            grouped_dict = adjust_group_targets_dict(grouped_dict, key, factor, dec)
    return grouped_dict

def adjust_group_targets_dict(grouped_dict: dict, key: tuple, factor: float, dec: int = 2) -> dict:
    # key => (side: str, tx: str, txi: int)
    if key in grouped_dict:
        _entry, _exit = grouped_dict[key]
        grouped_dict[key] = (round(_entry * factor, dec), round(_exit * factor, dec))
    return grouped_dict

def iterate_targets(query: dict, grouped_targets: dict):

    def _status(t1, t2) -> str:
        if not t1:
            return "pending"
        return "closed" if t1 and t2 else "open"

    res = {}
    for k, v in grouped_targets.items():
        _open, _close =  query_order_pair(query, v)
        close = _close if _close else query["end"]
        res[k] = {"status": _status(_open, _close), "open_close": (_open, close), "entry_exit":v, "ddtms_ddv":(None, None)}
        if res[k]["status"] == "closed" or res[k]["status"] == "open":
            res[k]["ddtms_ddv"] = get_drawdown(query, *v, _open, close)
    return res

def get_drawdown(_meta: dict, entry: float, exit: float, open: int, close: int) -> tuple:
    """get lowest if buy, or highest if sell value between openeing and closing timestamp:
    return (timestamp, lowest/highest"""
    meta = _meta.copy()
    meta["start"] = open
    meta["end"] = close

    if entry < exit:
        return list(query_drawdown(meta, direction="buy").tuples())[0]
    return list(query_drawdown(meta, direction="sell").tuples())[0]



def range_test(streamly, start, end, freq="1D", a=0.5, cond_adj: dict=None):

    """Perform a range test on entry and exit levels called by streamly every freq.
    :param cond_adj: dict = {(side, tx, txi): 0. - 1.}"""

    # load load data
    _ = streamly.gather(start=start, end=end)

    # run
    starts = streamly._date_iterator(start, end, freq=freq)
    result = {}
    for start in starts:
        resp = iterate_targets(streamly.gen_query(start), batch_adjustments(group_targets_to_dict(*streamly.get_levels(start), a=a), cond_adj))
        resp = {(start, *k): match_eval(v, streamly._last_price, streamly._last_tms) for k, v in resp.items()}
        result = {**result, **resp}
        print(f"finished {start}")
    return result



#----------------- EVALUATION Helper

def match_eval(trade: dict, ref_price: float, ref_time: int) -> dict:
    if trade["status"] == "closed":
        return eval_closed(trade)
    elif trade["status"] == "open":
        return eval_open(trade, ref_price, ref_time)
    return eval_pending(trade)

def eval_closed(trade):
    trade["pnl"] = calc_roi(*trade["entry_exit"])
    trade["duration"] = calc_duration(*trade["open_close"])
    return trade

def eval_open(trade, ref_price, ref_time):
    trade["pnl"] = calc_loss(*trade["entry_exit"], ref_price)
    trade["duration"] = calc_duration(trade["open_close"][0], ref_time)
    return trade


def eval_pending(trade):
    trade["pnl"] = None
    trade["duration"] = None
    return trade

def calc_duration(t1, t2):
    """duration in min from timestamps in ms"""
    return (t2 - t1) / 60000


def calc_roi(p1, p2):
    return round(abs(p2 - p1)/p1, 4)

def calc_loss(p1, p2, pr):
    if p1 < p2:
        return round((pr - p1) / p1, 4)
    return round((p1 - pr) / p1, 4)

def aggregate_trades(trade_records: dict) -> list[dict]:
    def calc_dd(k, v):
        if v["status"] == "pending":
            v["dd"] = 0
        else:
            if k[1] == "buy":
                v["dd"] = (v["ddtms_ddv"][1] - v["entry_exit"][1]) / v["entry_exit"][1]
            else:
                v["dd"] = (v["entry_exit"][1] - v["ddtms_ddv"][1]) / v["entry_exit"][1]
        return v

    def split_tuple_records(rec: dict) -> dict:
        _rec = rec.copy()
        for k, v in rec.items():
            if isinstance(v, tuple):
                _rec.pop(k)
                if "_" in k:
                    [k1, k2, *_] = k.split("_")
                else:
                    k1 = k + "_1"
                    k2 = k + "_2"
                _rec[k1] = v[0]
                _rec[k2] = v[1]
        return _rec

    def reduce_key(key, value) -> dict:
        _value = value.copy()
        _value["created_at"] = key[0]
        _value["side"] = key[1]
        _value["tx"] = key[2]
        _value["txi"] = key[3]
        return _value

    rec = {k: calc_dd(k, v) for k, v in trade_records.items()}
    rec = {k: split_tuple_records(v) for k, v in rec.items()}
    return [reduce_key(k, v) for i, (k, v) in enumerate(rec.items())]

def agg_trades_df(trade_records: dict) -> pd.DataFrame:
    return pd.DataFrame(aggregate_trades(trade_records)).sort_values("open")