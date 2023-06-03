from dataclasses import dataclass, field
from datetime import datetime
import ccxt
import numpy as np
import Misso.services.helper as mh
from Misso.models import Order, CustomId

@dataclass
class OrderPair:
    symbol: str = None
    sub: str = None
    wave_pair: tuple = None
    target_factor: float = None

    custom_id: str = None
    entry_price: float = None
    entry_size: float = None
    exit_price: float = None
    exit_size: float = None
    side: str = None
    entry: Order = None
    exit: Order = None
    entry_open: bool = False
    exit_open: bool = False

    def __post_init__(self):
        self.generate_orders()

    def generate_orders(self):
        self.side = self.parse_side(self.wave_pair)
        self.entry_price = self.wave_pair[0]
        self.custom_id = CustomId.generate(self.sub, self.symbol, type="entry", side=self.side)
        self.exit_price = self.parse_exit_price(self.wave_pair, self.target_factor)
        self.entry = Order(self.entry_size, self.entry_price, self.side, self.symbol, {"clientOrderId":self.custom_id})
        self.exit = Order(self.exit_size, self.exit_price, self.exit_side, self.symbol, {"triggerPrice":self.entry_price})

    def place_orders(self, exchange: ccxt, logger: object=None):
        exchange = mh.change_sub(exchange, self.sub)
        if not self.entry.is_open:
            if self.entry.create(exchange, logger=logger):
                self.entry_price = self.entry.price
                self.entry_size = self.entry.size
                self.entry_open = True
            else:
                return False
        else:
            self.entry_open = True
        if not self.exit.is_open:
            if self.exit.create(exchange, logger=logger):
                self.exit_price = self.exit.price
                self.exit_size = self.exit.size
                self.exit_open = True
            else:
                return False
        else:
            self.exit_open = True
        return True

    def is_open(self):
        self.entry_open = self.entry.is_open
        self.exit_open = self.exit.is_open

    def close_pair(self):
        self.entry.is_open = False
        self.entry.new_client_id()
        self.exit.is_open = False
        self.is_open()

    @property
    def exit_side(self):
        if self.side == "buy":
            return "sell"
        return "buy"

    @staticmethod
    def parse_exit_price(wave_pair, target_factor):
        if wave_pair[0] < wave_pair[1]:
            return wave_pair[0] + abs(wave_pair[1] - wave_pair[0]) * target_factor
        return wave_pair[0] - abs(wave_pair[1] - wave_pair[0]) * target_factor

    @staticmethod
    def parse_side(wave_pair):
        if wave_pair[0] < wave_pair[1]:
            return "buy"
        return "sell"

    @classmethod
    def from_wave_pair(cls, pos: object, wave_pair: tuple, size:float, target_factor:float=1):
        return cls(wave_pair=wave_pair, symbol=pos.symbol, sub=pos.subaccount, target_factor=target_factor, entry_size=size, exit_size=size)

    @classmethod
    def from_side_idx(cls, pos: object, side: str, idx: int, target_factor: float=1):
        wave_pair = pos.current_wave_pairs[pos.current_timeframe][side][idx]
        size = pos.current_base_size * idx if idx > 0 else pos.current_base_size
        return cls(wave_pair=wave_pair, symbol=pos.symbol, sub=pos.subaccount, target_factor=target_factor, entry_size=size, exit_size=size)

    @classmethod
    def from_dict(cls, dict):
        dict["entry"] = Order(**dict["entry"])
        dict["exit"] = Order(**dict["exit"])
        return cls(**dict)




