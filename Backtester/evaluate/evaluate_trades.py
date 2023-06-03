import pandas as pd
import numpy as np

def eval_rec_to_df(eval_rec: dict) -> pd.DataFrame:
    def flat_dict(k: any, v: dict, key_key: str = "index") -> dict:
        v[key_key] = k
        return v

    eval_df = pd.DataFrame([flat_dict(k, v) for k, v in eval_rec.items()])
    return eval_df

def evaluate_trades(df: pd.DataFrame, tx_to_size: dict={"1m": 10, "15m": 20, "1h": 40, "4h":80}, sell_fact: float=1, buy_fact: float=1, risk_value: float = 10000.0):

    df = get_order_size(df, tx_to_size, sell_fact, buy_fact)

    _open = [(t, p, side, size, size/p) for t, p, size, side in zip(df.open, df.entry, df.order_size, df.side)]
    _close = [(t, ex, side, size,-size/p) for t, p, size, side, ex in zip(df.close, df.entry, df.order_size, df.side, df.exit)]
    #


    dopen = {}

    for t, p, s, v, q in _open:
        if t not in dopen:
            dopen[t] = {"sell":[], "buy":[]}
        dopen[t][s].append((p, v, q))

    dclose = {}

    for t, p, s, v, q in _close:
        if t not in dclose:
            dclose[t] = {"sell":[], "buy":[]}
        dclose[t][s].append((p, v, q))


    txs = np.unique(list(dclose.keys()) + list(dopen.keys()))

    tx_rec = dopen.copy()
    for k, v in dclose.items():
        if k in tx_rec:
            tx_rec[k]["sell"] += v["sell"]
            tx_rec[k]["buy"] += v["buy"]
        else:
            tx_rec[k] = v

    tx_sorted = {t: tx_rec[t] for t in txs}

    d = {"p_cur": 0.0, "p_avg": 0.0, "qty": 0.0, "value": 0.0, "pnl": 0.0, "capital": 500.0, "calc_value":0.0}

    _long, _short = d.copy(), d.copy()
    rec_long, rec_short = {}, {}


    for x, v in tx_sorted.items():
        for tr in v["sell"]:
            _short = on_transaction(_short, tr, "sell", risk_value)
        rec_short[x] = _short
        for tr in v["buy"]:
            _long = on_transaction(_long, tr, "buy", risk_value)
        rec_long[x] = _long


    rec = {}
    for x in rec_short.keys():
        rec[x] = {
            "p_short": rec_short[x]["p_avg"],
            "qty_short": rec_short[x]["qty"],
            "val_short": rec_short[x]["calc_value"],
            "p_long": rec_long[x]["p_avg"],
            "p_lcur": rec_long[x]["p_cur"],
            "qty_long": rec_long[x]["qty"],
            "val_long": rec_long[x]["calc_value"],
            "pnl": rec_short[x]["pnl"] + rec_long[x]["pnl"],
            "capital": rec_short[x]["capital"] + rec_long[x]["capital"],
            "calc_value": rec_short[x]["calc_value"] + rec_long[x]["calc_value"]
        }
        rec[x]["total"] = rec[x]["capital"] + rec[x]["calc_value"]

    return rec

def get_order_size(df_orig: pd.DataFrame, tx_to_size: dict={"1m": 10, "15m": 20, "1h": 40, "4h":80}, sell_fact: float=1, buy_fact: float=1) -> pd.DataFrame:
    df = df_orig.copy()
    df["side_factor"] = np.where(df["side"] == "sell", sell_fact, buy_fact)
    df["order_size"] = [tx_to_size[k] for k in list(df.tx)]
    df["order_size"] = df["order_size"] * abs(df["txi"]) * df["side_factor"]
    return df


def calc_p_avg(d: dict):
    return d["value"] / d["qty"] if d["qty"] > 0.0 else 0.0

def delta_value(d: dict, qty: float):
    return d["p_avg"] * qty


def calc_pnl(d: dict, p: float , qty: float, side: str):
    if side == "sell":
        return pnl_short(d, p, qty)
    return pnl_long(d, p, qty)

def pnl_short(d: dict, p: float , qty: float):
    return (d["p_avg"] - p) * abs(qty)

def pnl_long(d: dict, p: float, qty: float):
    return (p - d["p_avg"]) * abs(qty)

# entries
def on_transaction(old: dict, tx: tuple, side: str, risk_value: float = 10000) -> dict:

    if tx[2] > 0:
        new = on_entry(old, tx, side)
        return after_transaction(new, tx, side, risk_value)
    if tx[2] < 0:
        new = on_exit(old, tx, side)
        return after_transaction(new, tx, side, risk_value)

def after_transaction(new: dict, tx: tuple, side: str, risk_value: float=10000.0) -> dict:
    if new["value"] > risk_value:
        if side == "sell" and new["p_cur"] < new["p_avg"]:
            return close_position(new, tx, side)
        if side == "buy" and new["p_cur"] > new["p_avg"]:
            return close_position(new, tx, side)
    return new

def on_entry(old: dict, tx: tuple, side: str) -> dict:
    new = old.copy()
    new["p_cur"] = tx[0]
    new["value"] += tx[1]
    new["qty"] += tx[2]
    new["p_avg"] = calc_p_avg(new)
    new["capital"] -= tx[1]
    new["calc_value"] = calc_value(new, side)
    return round_dict(new)

# exits
def on_exit(old: dict, tx: tuple, side: str) -> dict:
    new = old.copy()
    new["p_cur"] = tx[0]
    new["qty"] += tx[2]
    if new["qty"] < 0.0:
        return close_position(old, tx, side)
    new["value"]  += (dv := delta_value(new, tx[2]))
    new["pnl"] += (pnl := calc_pnl(new, tx[0], tx[2], side))
    new["capital"] += (-dv + pnl)
    new["calc_value"] = calc_value(new, side)
    return round_dict(new)

def close_position(old: dict, tx: tuple, side: str):
    new = old.copy()
    new["p_cur"] = tx[0]
    new["qty"] = 0.0
    new["value"] = 0.0
    new["pnl"] += (pnl := calc_pnl(new, tx[0], old["qty"], side))
    new["capital"] += old["value"] + pnl
    new["calc_value"] = 0
    return round_dict(new)

def calc_value(d: dict, side) -> float:
    if side == "sell":
        return (2*d["p_avg"] - d["p_cur"]) * d["qty"]
    return d["p_cur"] * d["qty"]


def round_dict(old: dict, key_dec=None) -> dict:
    key_dec = {"p_cur": 2, "p_avg": 2, "qty": 16, "value": 2, "pnl": 2, "capital": 2, "calc_value":2} if not key_dec else key_dec
    return {k: round(v, key_dec[k]) for k, v in old.items()}

def get_xy(rec: dict, key: str) -> list[tuple]:
    return [(x, v[key]) for x, v in rec.items()]

def get_x_y(rec, key) -> tuple:
    xy = get_xy(rec, key)
    return [x for (x, y) in xy], [y for (x, y) in xy]

