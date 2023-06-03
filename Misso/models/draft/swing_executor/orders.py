from dataclasses import dataclass, field
from itertools import count
from datetime import datetime
import ccxt
import uuid
import Misso.services.helper as mh
from Misso.models.draft.swing_executor.order_pairs import CustomId

counter = count()

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







@dataclass
class Order(OrderGenerator):
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

    def set_order_opening(self, response: dict=None):
        if isinstance(response, dict):
            self.id = response["id"]
            self.status = "new"
            self.is_placed = True
            self.is_open = True
        if response is None or response == "failed":
            self.status = "failed"


    def create(self, exchange: ccxt):
        is_valid = self.safe_format(exchange)
        if not is_valid:
            self.status = "failed"
            print("not valid size")
            return False

        type = "limit" if "triggerPrice" not in self.params else "stop"
        try:
            response = exchange.create_order(self.symbol, type, self.side, self.size, self.price, params=self.params)
            self.set_order_opening(response)
            return True
        except Exception as e:
            print(f"ERROR executing order for {self.symbol}: {self.client_id}")
            try:
                response = exchange.create_order(self.symbol, type, self.side, self.size, self.price, params=self.params)
                self.set_order_opening(response)
                return True
            except:
                self.set_order_opening("failed")
                return False


    def safe_format(self, exchange: ccxt, size_treshhold: float=2):
        size = self.safe_size(exchange, self.symbol, self.size, size_treshhold)
        print(f"for {self.symbol} size is {size}")
        if size is None:
            return False
        self.size = size
        self.price = float(exchange.price_to_precision(self.symbol, self.price))
        if self.trigger_price:
            self.trigger_price = float(exchange.price_to_precision(self.symbol, self.trigger_price))
            self.params["triggerPrice"] = self.trigger_price
        return True

    @staticmethod
    def safe_size(exchange: ccxt, symbol, size, size_treshhold:float=2):
        size = float(exchange.amount_to_precision(symbol, size))
        min_size = exchange.markets[symbol]["limits"]["amount"]["min"]
        if size_treshhold * size >= min_size:
            size = size if size > min_size else min_size
            return size
        return None


    def parse_update(self, response):
        if isinstance(response, dict) and not self.is_updated:
            if self.status != response["status"]:
                self.status = response["status"]
                self.is_updated = True
            elif self.filled != response["filled"]:
                self.filled = response["filled"]
                self.remaining = response["remaining"]
                self.is_updated = True

