import time
import ccxt
import numpy as np
from dataclasses import dataclass, field
from Misso.services.helper import parse_side, parse_exit_side
import Misso.services.helper as mh


@dataclass
class Position:
    symbol: str
    size_increment: float
    min_amount: float
    price_increment: float
    side: str = None
    size: float = 0.0
    net_size: float = 0.0
    last_net: float = 0.0
    dca_count: int = 0
    notional: float = 0.0
    unrealized: float = 0.0
    break_even: float = 0.0
    buy_order: list = None
    buy_status: str = "not_set"
    sell_order: list = None
    sell_status: str = "not_set"
    timestamp: int = int(time.time())*1000
    current_range: list = field(default_factory=list)
    current_min_max: list = field(default_factory=list)
    order_size: float = 0.0
    next_order_size: float = 0.0
    is_open: bool = False
    is_pending: bool = False
    has_fills: bool = False
    trade_count: int = 0
    is_priority: bool = False

    def update(self, response):
        _side = self.side
        _size = self.size
        self.size = response["contracts"]
        self.side = parse_side[response["side"]] if self.size > 0 else self.side
        self.net_size = self.size * 1 if self.side == "buy" else self.size * -1
        self.notional = response["notional"]
        self.unrealized = response["unrealizedPnl"]
        self.set_break_even(response)

    def open(self, exchange: ccxt):
        self.dca_count = 1
        self.current_range = mh.get_next_dca_range(exchange, self)
        self.is_pending = False
        self.is_open = True
        self.has_fills = False
        self.cancel_all(exchange)
        print(f"OPEN {self.get_position_stats()}")

    def close(self, exchange: ccxt):
        self.dca_count = 0
        self.trade_count += 1
        if self.next_order_size > 0:
            self.order_size = self.next_order_size
            self.next_order_size = 0
        self.set_current_range(exchange, direction=self.get_exit_side())
        self.is_pending = False
        self.is_open = False
        self.has_fills = False
        self.cancel_all(exchange)
        print(f"CLOSE {self.get_position_stats()}")

    def dca(self, exchange: ccxt):
        self.dca_count += 1
        self.current_range = mh.get_next_dca_range(exchange, self)
        self.has_fills = False
        self.cancel_all(exchange)
        print(f"DCA {self.get_position_stats()}")

    def reset_orders(self):
        self.buy_order = None
        self.buy_status = "not_set"
        self.sell_order = None
        self.sell_status = "not_set"

    def generate_dca_order(self, exchange: ccxt):
        self.dca_count = self.dca_count if self.dca_count > 0 else 1
        size = self.order_size * self.dca_count
        price = self.current_range[0] if self.side == "buy" else self.current_range[1]
        price = float(exchange.price_to_precision(self.symbol, price))
        return [0, size, price, self.side, self.symbol]

    def generate_exit_order(self, exchange: ccxt,  profit_target: float):
        if self.break_even is None:
            self.update(mh.get_position_update(exchange, self.symbol))
            if self.break_even is None:
                self.break_even = float(mh.get_position_update(exchange, self.symbol)["info"]["recentBreakEvenPrice"])
        if self.break_even is None:
            print(f"BREAK EVEN is None for {self.symbol}")
        price = self.break_even * (1+profit_target) if self.side == "buy" else self.break_even * (1-profit_target)
        price = float(exchange.price_to_precision(self.symbol, price))
        side = "buy" if self.side == "sell" else "sell"
        return [0, self.size, price, side, self.symbol]


    ###########
    ### GETTER
    def get_exit_side(self):
        if not self.is_open:
            print(f"ERROR {self.symbol} get_exit_side() position is not open")
        if self.side is not None:
            return parse_exit_side[self.side]
        return None

    def get_exit_order(self):
        if self.side == "buy":
            return self.sell_order
        return self.buy_order

    def get_exit_status(self):
        if self.side == "buy":
            return self.sell_status
        return self.buy_status

    def get_dca_side(self):
        if not self.is_open:
            print(f"ERROR {self.symbol} get_exit_side() position is not open")
        return self.side

    def get_dca_order(self):
        if self.side == "buy":
            return self.buy_order
        return self.sell_order

    def get_dca_status(self):
        if self.side == "buy":
            return self.buy_status
        return self.sell_status

    def get_position_stats(self):
        if self.is_open and self.break_even > 0 and self.buy_order is not None and self.sell_order is not None:
            delta_exit = round(abs(self.break_even - self.get_exit_order()[2])/self.break_even, 4)
            delta_dca = round(abs(self.break_even - self.get_dca_order()[2])/self.break_even, 4)
        else:
            delta_dca, delta_exit = 0, 0

        return f"[POSITION - {self.symbol}] side: {self.side}, size: {self.size} BreakEven: {self.break_even}, Exit%: {delta_exit}, Dca%: {delta_dca}, Dca no. {self.dca_count}\n" \
              f"[POSITION - {self.symbol}] exit: {self.get_exit_status()}, dca: {self.get_dca_status()}, is_open: {self.is_open}, is_pending: {self.is_pending}, current_range: {self.current_range}"

    ###########
    ### SETTER
    def set_break_even(self, response):
        # if not self.notional is None:
        #     if not self.unrealized is None:
        #         self.break_even = (self.notional + self.unrealized)/self.size
        #     else:
        #         self.break_even = float(response["info"]["recentBreakEven"])
        # else:
        try:
            self.break_even = float(response["info"]["recentBreakEvenPrice"])
        except:
            self.break_even = response["entryPrice"]


    def set_current_range(self, exchange: ccxt, price: float=None, direction: str = None, init_delta: float=0.005):
        if price is None:
            price = exchange.fetch_ticker(self.symbol)["last"]
        if direction is None:
            high, low = mh.next_higher_lower_in_timeframes(exchange, self.symbol, price, treshhold=init_delta)
        else:
            if direction == "sell":
                _range = mh.get_dca_levels(exchange, self.symbol, price, "buy", 1, init_treshhold=init_delta)
            else:
                _range = mh.get_dca_levels(exchange, self.symbol, price, "sell", 1, init_treshhold=init_delta)
            high = max(_range)
            low = min(_range)
        low = high * 0.98 if low is None else low
        high = low * 1.02 if high is None else high
        try:
            self.current_range = [float(exchange.price_to_precision(self.symbol, low)), float(exchange.price_to_precision(self.symbol, high))]
        except:
            print(f"ERROR DECIMAL TO PRECISION high: {high}, low: {low}")

    def set_shifted_range(self,exchange: ccxt, price: float, direction: str, min_profit_range: float=0.0045):
        if len(self.current_range) < 2:
            self.set_current_range(exchange, price, init_delta=0.01)
        distance = self.current_range[1] - self.current_range[0]
        if direction == "buy":
            dhigher = (self.current_range[1] - price) / price
            if dhigher < min_profit_range:
                price = self.current_range[1] * (1 - min_profit_range)
            price = price - self.price_increment
            self.current_range = [price, price+distance]
        elif direction == "sell":
            dlower = (price - self.current_range[0]) / price
            if dlower < min_profit_range:
                price = self.current_range[0] * (1 + min_profit_range)
            price = price + self.price_increment
            self.current_range = [price - distance, price]



    ##########
    ### ORDER

    def update_order_status(self, exchange: ccxt):
        if self.buy_order is not None and self.buy_status not in ["canceled", "closed", "failed"]:
            try:
                self.buy_status = exchange.fetch_order(self.buy_order[0])["status"]
            except:
                self.buy_status = "failed"
                print(f"failed updating order for {self.buy_order}")
        if self.sell_order is not None and self.sell_status not in ["canceled", "closed", "failed"]:
            try:
                self.sell_status = exchange.fetch_order(self.sell_order[0])["status"]
            except:
                self.sell_status = "failed"
                print(f"failed updating order for {self.sell_order}")

    def cancel_exit_side(self, exchange: ccxt):
        exchange.cancel_all_orders(self.symbol, params={"side":self.get_exit_side()})

    def cancel_exit_order(self, exchange: ccxt):
        try:
            exchange.cancel_order(self.get_exit_order()[0])
        except:
            self.cancel_exit_side(exchange)

    def cancel_dca_side(self, exchange: ccxt):
        exchange.cancel_all_orders(self.symbol, params={"side":self.side})

    def cancel_dca_order(self, exchange: ccxt):
        try:
            exchange.cancel_order(self.get_dca_order()[0])
        except:
            self.cancel_dca_side(exchange)

    def cancel_all(self, exchange: ccxt, reset: bool=True):
        exchange.cancel_all_orders(self.symbol)
        if reset:
            self.reset_orders()
