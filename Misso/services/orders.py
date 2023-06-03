from datetime import datetime
import ccxt
from typing import List, Dict
import numpy as np
import ccxt
import Misso.services.helper as mh
from Misso.models import Order, Position, CustomId



class Create:


    @staticmethod
    def order_pair(symbol, side, amount, entry, exit_target, target_factor: float = 1, src_id: str=None, double_exit=False, increment: float=0, **kwargs):
        entry_params = CustomId.generate_params(src_id, symbol, type="entry", side=side, **kwargs)
        exit_params = {"triggerPrice":entry+increment}
        entry_order = Order(amount, entry, side, symbol, entry_params)
        exit_amount = amount*2 if double_exit else amount
        exit_price = entry + abs(entry - exit_target) * target_factor if side == "buy" else entry - abs(entry - exit_target) * target_factor
        exit_order = Order(exit_amount, exit_price, mh.parse_exit_side[side], symbol, exit_params)
        return entry_order, exit_order

    @staticmethod
    def _order_pair(symbol, side, amount, entry, exit_target, target_factor: float = 1, src_id: str=None, double_exit=False, increment: float=0, **kwargs):
        entry_params = CustomId.generate_params(src_id, symbol, type="entry", side=side, **kwargs)
        exit_params = {"triggerPrice":entry+increment}
        entry_order = Order(amount, entry, side, symbol, entry_params)
        exit_amount = amount*2 if double_exit else amount
        exit_price = entry + abs(entry - exit_target) * target_factor if side == "buy" else entry - abs(entry - exit_target) * target_factor
        exit_order = Order(exit_amount, exit_price, mh.parse_exit_side[side], symbol, exit_params)
        return entry_order, exit_order








class OrderGenerator(CustomId):
    @staticmethod
    def create_auto_order(symbol, side, amount, entry, exit_target, target_factor: float = 1, src_id: str=None, double_exit=False, increment: float=0, **kwargs):
        entry_params = CustomId.generate_params(src_id, symbol, type="entry", side=side, **kwargs)
        exit_params = {"triggerPrice":entry+increment}
        exit_params = CustomId.add_client_id(exit_params, src_id, symbol, type="exit",side=side, **kwargs)
        entry_order = Order(amount, entry, side, symbol, entry_params)
        exit_amount = amount*2 if double_exit else amount
        exit_price = entry + abs(entry - exit_target) * target_factor if side == "buy" else entry - abs(entry - exit_target) * target_factor
        exit_order = Order(exit_amount, exit_price, mh.parse_exit_side[side], symbol, exit_params)
        return entry_order, exit_order

    @staticmethod
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

    @staticmethod
    def create_wave_orders_by_position(exchange: ccxt, pos: object, side: str="both", timeframe=None, target_factor=0.5):
        sides = ["buy", "sell"] if side == "both" else [side]
        w_orders = {}
        for s in sides:
            w_orders[s] = {}
            w_orders = OrderGenerator.create_orders_on_waves_side(exchange, pos.symbol, pos.current_wave_pairs[pos.current_timeframe], pos.current_base_size, s, w_orders, sub=pos.subaccount, target_factor=target_factor, tx=pos.current_timeframe)
        return w_orders

    @staticmethod
    def create_order_pairs_by_closed_list(exchange: ccxt, pos: object, side: str="both", timeframe=None, target_factor=0.5):
        w_orders = {}
        for s in sides:
            w_orders[s] = {}
            w_orders = OrderGenerator.create_orders_on_waves_side(exchange, pos.symbol, pos.closed_wave_pairs[pos.current_timeframe], pos.current_base_size, s, w_orders, sub=pos.subaccount, target_factor=target_factor, tx=pos.current_timeframe)
        return w_orders

    @staticmethod
    def create_orders_on_waves_side(exchange: ccxt, symbol: str, wave_pairs: dict, base_size: float, side: str=None, out: dict=None, last: float=None, only_limit:bool=True, sub: str="subacc", target_factor:float = 0.5, **kwargs):
        inc = mh.increment_by_side(exchange, symbol, side)
        last = mh.safe_last_price(exchange, symbol, last)
        wps = wave_pairs[side]
        d = {side:{}} if not out else out
        for idx, wp in wps.items():
            w, tw = wp[0], wp[1]
            if only_limit:
                if (side == "buy" and w > last) or (side=="sell" and w < last):
                    continue
            size = base_size * idx if idx > 0 else base_size
            d[side][idx] = OrderGenerator.create_auto_order(symbol, side, size, w, tw, target_factor, src_id=sub, increment=inc, wx=idx, **kwargs)
        return d

    @staticmethod
    def change_sub(exchange, subaccount):
        from Misso.config import API_CONFIG
        if subaccount in API_CONFIG:
            exchange.headers["FTX-SUBACCOUNT"] = subaccount
            return exchange
        else:
            print("subaccount not in exchange_config.yaml")

    @staticmethod
    def create_order_by_sub(exchange, subaccount, order, params=None):
        if exchange is None:
            exchange = mh.initialize_exchange_driver("Main")
        exchange = OrderGenerator.change_sub(exchange, subaccount)
        params = CustomId.add_client_id(params, subaccount, order[0])
        return exchange.create_limit_order(*order, params=params)