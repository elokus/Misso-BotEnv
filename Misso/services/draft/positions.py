import Misso.services.helper as ph
from dataclasses import dataclass, field, InitVar
from itertools import count
from datetime import datetime
from typing import List, Dict

counter = count()


@dataclass
class Position:
    symbol: str
    size: float = 0.0
    cost: float = 0.0
    value: float = 0.0
    break_even: float = 0.0
    avg_price: float = 0.0
    side: str = None
    is_open: bool = False
    is_pending: bool = False
    unit_size: float = 0.0
    ref_price: float = 0.0
    dca_count: int = 0
    dca_levels: list = field(default_factory=list)
    opening_range: list = field(default_factory=list)
    exit_target: float = 0.0
    current_exit_size: float = 0.0
    dca_order_set: bool = False
    exit_order_set: bool = False
    opened_at: datetime = field(default_factory= datetime.utcnow)
    index: int = field(default_factory=lambda: next(counter))

    def __post_init__(self):
        return

    def sync_open_positions(self, exchange: object, pos_update: dict):
        if not self.is_open:
            self.parse_update_dict(pos_update)
            self.update_current_range(exchange)
            self.update_dca_level(exchange)
            self.update_dca_count()
        else:
            _size = self.size
            self.parse_update_dict(pos_update)
            if _size != self.size:
                self.update_dca_count()

    def parse_update_dict(self, updates: dict):
        for key, value in updates.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def update_current_range(self, exchange, ref_price=None, current_range=None):
        self.ref_price = exchange.fetchTicker(self.symbol)["last"] if ref_price is None else ref_price
        self.opening_range = ph.find_outbreak_target(exchange, self.symbol, self.ref_price) if current_range is None else current_range

    def update_dca_level(self, exchange, ref_price=None):
        if self.is_open:
            self.break_even = exchange.fetchTicker(self.symbol)["last"] if ref_price is None else ref_price
            self.dca_levels = ph.get_dca_levels(exchange, self.symbol, self.break_even, self.side)

    def update_dca_count(self):
        self.dca_count = int(self.size/self.unit_size) if self.unit_size > 0 else 0


@dataclass
class Positions:
    watch_list: InitVar[list]
    dict: Dict[str, Position] = field(init=False, default_factory=dict)

    def __post_init__(self, watch_list):
        for symbol in watch_list:
            self.dict[symbol] = Position(symbol)

    def parse_live_positions(self,exchange: object, live_positions: dict):
        for key, value in live_positions.items():
            pos_update = ph.formatted_position_update(value)
            if key in self.dict:
                self.dict[key].sync_open_positions(exchange, pos_update)
            else:
                self.dict[key] = Position(key)
                self.dict[key].sync_open_positions(exchange, pos_update)

    def update_unit_sizes(self, exchange, target_unit_factor, total_capital):
        updates = ph.get_unit_size_dict(exchange, target_unit_factor, total_capital)
        for key, value in updates.items():
            if key in self.dict:
                self.dict[key].parse_update_dict(value)

    def return_list(self):
        return list(self.dict.values())





    # def parse_filled_order(self, order):
    #     _size = order.filled - order._filled
    #     if self.side == order.side:
    #         self.size += _size
    #         self.cost += _size * order.price
    #         self.avg_price = self.cost / self.size if self.size > 0 else 0

