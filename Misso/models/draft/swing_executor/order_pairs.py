from dataclasses import dataclass, field
from itertools import count
from datetime import datetime
import ccxt
from typing import List, Dict
import numpy as np
import ccxt
import uuid
import Misso.services.helper as mh

counter = count()

class CustomId:
    @staticmethod
    def generate(caller_id: str, ref_id: str=None, **kwargs):
        import uuid
        _id = [caller_id, str(uuid.uuid4())]
        if ref_id:
            _id.append(ref_id)
        _spez = {key: str(arg) for key, arg in kwargs.items()}
        if len(_spez) > 0:
            _id.append(CustomId.parse_from_list(_spez, list_sep="-"))
        return CustomId.parse_from_list(_id)

    @staticmethod
    def generate_params(subaccount: str, symbol: str, **kwargs):
        return {"clientId": CustomId.generate(subaccount, symbol, **kwargs)}

    @staticmethod
    def add_client_id(params: dict, subaccount: str, symbol: str, **kwargs):
        params = {} if params is None else params
        params["clientId"] = CustomId.generate(subaccount, symbol, **kwargs)
        return params

    @staticmethod
    def _format_string(element: list, list_sep: str="_", dict_sep: str="-", dict_elem_sep: str="%"):
        if isinstance(element, list):
            e = list_sep.join(element)
        elif isinstance(element, dict):
            _e = [dict_elem_sep.join([str(k), str(v)]) for k, v in element.items()]
            e = dict_sep.join(_e)
        else:
            print(f"HELPER:PY   _format_string e is neither list nor dict: {element}")
        return e

    @staticmethod
    def parse_from_list(id_container: list, list_sep: str="_", dict_sep: str="-", dict_elem_sep: str="%"):
        _id = []
        if isinstance(id_container, list):
            for id in id_container:
                if isinstance(id, list):
                    e = CustomId._format_string(id, list_sep, dict_sep, dict_elem_sep)
                elif isinstance(id, dict):
                    e = CustomId._format_string(id, list_sep, dict_sep, dict_elem_sep)
                else:
                    e = str(id)
                _id.append(e)
        elif isinstance(id_container, dict):
            _id = {}
            for key, id in id_container.items():
                if isinstance(id, list):
                    e = CustomId._format_string(id, list_sep, dict_sep, dict_elem_sep)
                elif isinstance(id, dict):
                    e = CustomId._format_string(id, list_sep, dict_sep, dict_elem_sep)
                else:
                    e = str(id)
                _id[key] = e
        return CustomId._format_string(_id, list_sep, dict_sep, dict_elem_sep)

    @staticmethod
    def update(old_id: str, ref_id: str, update_spez: bool=False, **kwargs):
        if len(old_id.split("_")) >= 3:
            src_id, uid, _ref_id, *_spez = old_id.split("_")
            ref_id = _ref_id if ref_id is None else ref_id
            if update_spez:
                _d = {e.split("%")[0] : e.split("%")[1] for e in _spez.split("-") if "%" in e}
                for k, v in kwargs.items():
                    _d[k] = str(v)
        else:
            src_id, uid = old_id.split("_")
            _d = {k: str(v) for k, v in kwargs.items()}
        new_id = [src_id, uid, ref_id, _d] if len(_d) > 0 else [src_id, uid, ref_id]
        if len(_d) > 0:
            new_id.append(_d)
        return CustomId.parse_from_list(new_id)

    @staticmethod
    def parse_from_string(client_id):
        src, uid, ref, *spez = client_id.split("_")
        if len(spez) > 0:
            spez = {e.split("%")[0]: e.split("%")[1] for e in spez[0].split("-")}
        return {
            "src_id": src,
            "uid": uid,
            "ref_id": ref,
            "spez": spez
        }
    @staticmethod
    def parse_from_dict(d: dict):
        return CustomId.parse_from_list([v for v in d.values()])



class OrderPairs:
    def __init__(self, *args, **kwargs):
        self.pair_id = str(uuid.uuid4())





def create_auto_order(symbol, side, amount, entry, exit_target, target_factor: float = 1, base_id: str=None, double_exit=False, increment: float=0):
    if not base_id:
        base_id = str(uuid.uuid4())
    entry_params = {"clientId": base_id + "_entry"}
    exit_params = {"clientId":base_id + "_exit", "triggerPrice":entry+increment}
    entry_order = Order(amount, entry, side, symbol, entry_params)
    exit_amount = amount*2 if double_exit else amount
    exit_price = entry + abs(entry - exit_target) * target_factor if side == "buy" else entry - abs(entry - exit_target) * target_factor
    exit_order = Order(exit_amount, exit_price, mh.parse_exit_side[side], symbol, exit_params)
    return entry_order, exit_order

def exit_on_break_even(exchange: ccxt, position: object, profit_target: float, cancel_side: bool=True, reduce_only: bool=True):
    if not position.size > 0:
        return
    if cancel_side:
        position.cancel_exit_side(exchange)
    profit_target = -profit_target if position.side == "sell" else profit_target
    price = float(exchange.price_to_precision(position.symbol, position.break_even*(1+profit_target)))
    size = float(exchange.amount_to_precision(position.symbol, position.size))
    params = {"clientId": str(uuid.uuid4()) + "_close", "reduceOnly":reduce_only}
    return Order(size, price, position.get_exit_side(), position.symbol, params)

def create_wave_orders_by_position(exchange: ccxt, pos: object, side: str="both", timeframe=None):
    sides = ["buy", "sell"] if side == "both" else [side]
    w_orders = {}
    for s in sides:
        w_orders[s] = {}
        w_orders = create_orders_on_waves_side(exchange, pos.symbol, pos.get_wave_pairs()[timeframe], pos.current_base_size, s, w_orders, target_factor=0.5)
    return w_orders

def create_orders_on_waves_side(exchange: ccxt, symbol: str, wave_pairs: dict, base_size: float, side: str=None, out: dict=None, last: float=None, only_limit:bool=True, target_factor:float = 0.5):
    inc = mh.increment_by_side(exchange, symbol, side)
    last = mh.safe_last_price(exchange, symbol, last)
    wps = wave_pairs[side]
    d = {side:{}} if not out else out
    for idx, w, tw in wps.items():
        if only_limit:
            if (side == "buy" and w > last) or (side=="sell" and w < last):
                continue
        size = base_size * (idx+1)
        d[side][idx] = create_auto_order(symbol, side, size, w, tw, target_factor, increment=inc)
    return d









@dataclass
class Order:
    size: float
    price: float
    side: str
    symbol: str
    params: dict
    id: str = None
    client_id: str = None
    status: str = "generated"
    filled: float = 0.0
    _filled: float = 0.0
    trigger_price: float = None
    remaining: float = field(init=False)
    value: float = field(init=False)
    opened_at: datetime = field(default_factory = datetime.utcnow)
    index: int = field(default_factory=lambda: next(counter))
    is_conditional = False
    closed_at: int = None
    is_placed: bool = False
    is_open: bool = False
    is_closed: bool = False
    is_canceled: bool = False

    is_processed: bool = False
    is_partially_filled: bool = False
    is_updated: bool = False


    def __post_init__(self):
        self.remaining = self.size
        self.value = self.size * self.price
        self.trigger_price = self.params["triggerPrice"] if "triggerPrice" in self.params else None
        self.client_id = self.params["clientId"] if "clientId" in self.params else None

    def create(self, exchange: ccxt):
        self.safe_format(exchange)
        type = "limit" if "triggerPrice" not in self.params else "stop"
        response = exchange.create_order(self.symbol, type, self.side, self.size, self.price, params=self.params)
        self.id = response["id"]
        self.status = "new"
        self.is_placed = True
        self.is_open = True
        return response

    def safe_format(self, exchange: ccxt):
        self.size = float(exchange.amount_to_precision(self.symbol, self.size))
        self.price = float(exchange.price_to_precision(self.symbol, self.price))
        if self.trigger_price:
            self.trigger_price = float(exchange.price_to_precision(self.symbol, self.trigger_price))
            self.params["triggerPrice"] = self.trigger_price


    def parse_update(self, response):
        if isinstance(response, dict) and not self.is_updated:
            if self.status != response["status"]:
                self.status = response["status"]
                self.is_updated = True
            elif self.filled != response["filled"]:
                self.filled = response["filled"]
                self.remaining = response["remaining"]
                self.is_updated = True
