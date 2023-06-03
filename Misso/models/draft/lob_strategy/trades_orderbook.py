import pandas as pd
import numpy as np
from dataclasses import dataclass, field, InitVar

@dataclass
class Trades:
    symbol: str
    last_trades: list
    buy_size: float = 0.0
    sell_size: float = 0.0
    avg_price: float = 0.0
    pace: float = 0.0
    price_range: list = field(default_factory=list)
    prices: list = field(default_factory=list)

    def __post_init__(self):
        self.pace = abs(pd.to_datetime(self.last_trades[0][0]["time"]) - pd.to_datetime(self.last_trades[-1][0]["time"])).total_seconds()
        sell_size, buy_size = 0.0, 0.0
        prices = []
        for trade in self.last_trades:
            buy_size += trade[0]["size"] if trade[0]["side"] == "buy" else 0
            sell_size += trade[0]["size"] if trade[0]["side"] == "sell" else 0
            prices.append(trade[0]["price"])
        self.price_range = [min(prices), max(prices)]
        self.buy_size = buy_size
        self.sell_size = sell_size
        self.prices = prices

    def update(self, last_trades):
        self.last_trades = last_trades
        self.pace = abs(pd.to_datetime(last_trades[0][0]["time"]) - pd.to_datetime(last_trades[-1][0]["time"])).total_seconds()
        sell_size, buy_size = 0.0, 0.0
        prices = []
        for trade in self.last_trades:
            buy_size += trade[0]["size"] if trade[0]["side"] == "buy" else 0
            sell_size += trade[0]["size"] if trade[0]["side"] == "sell" else 0
            prices.append(trade[0]["price"])
        self.price_range = [min(prices), max(prices)]
        self.buy_size = buy_size
        self.sell_size = sell_size
        self.prices = prices

    def get_pace_volume(self):
        return (self.buy_size+self.sell_size)/len(self.last_trades)*50

    def get_price_range(self):
        self.update()

@dataclass
class Orderbook:
    symbol: str
    tick: float
    ob: InitVar[dict]
    asks: list = field(default_factory=list)  #sells - max
    bids: list = field(default_factory=list)  #buys  - min
    mean: float = 0.0

    def __post_init__(self, ob: dict):
        self.update(ob)
        # self.asks = o["asks"]
        # self.bids = orderbook["bids"]
        # self.mean = (self.asks[0][0] + self.bids[0][0])/2

    def update(self, orderbook: dict):
        self.asks = orderbook["asks"]
        self.bids = orderbook["bids"]
        self.mean = (self.asks[0][0] + self.bids[0][0])/2


    def get_next_ask_wall(self,  ref_vol: float):
        for entry in self.asks:
            if entry[1] > ref_vol:
                return entry[0]

    def get_next_bid_wall(self,  ref_vol: float):
        for entry in self.bids:
            if entry[1] > ref_vol:
                return entry[0]

    def get_range(self, ref_vol: float):
        return [self.get_next_bid_wall(ref_vol), self.get_next_ask_wall(ref_vol)]

    def abs_range(self, ref_vol: float):
        _range = self.get_range(ref_vol)
        return abs(_range[0]-_range[1])

    def pct_range(self, ref_vol: float):
        return self.abs_range(ref_vol)/self.mean

    def get_sell_price(self, ref_vol: float):
        return self.get_next_ask_wall(ref_vol)-self.tick

    def get_buy_price(self, ref_vol: float):
        return self.get_next_bid_wall(ref_vol)+self.tick