import Misso.services.helper as ph
from dataclasses import dataclass, field, InitVar
from itertools import count
from datetime import datetime
from typing import List, Dict

counter = count()


@dataclass
class Position:
    symbol: str
    unit_param: InitVar[dict]
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
    last_change: str = "init"
    opened_at: datetime = field(default_factory=datetime.utcnow)
    index: int = field(default_factory=lambda: next(counter))

    def __post_init__(self, unit_param: dict):
        for key, value in unit_param.items():
            if hasattr(self, key):
                setattr(self, key, value)

    def parse_update_dict(self, updates: dict):
        before = self.return_dict()
        changes, is_change = ph.changes_position_update(before, updates)
        if is_change:
            for key, value in updates.items():
                if hasattr(self, key):
                    setattr(self, key, value)
            self.update_dca_count()
        return changes

    def update_dca_count(self):
        self.dca_count = int(self.size/self.unit_size) if self.unit_size > 0 else 0

    def return_dict(self):
        return self.__dict__.copy()

    def close(self):
        reset_tmpl = ph.reset_position_tmpl_dict()
        self.parse_update_dict(reset_tmpl)

@dataclass
class Positions:
    unit_params: InitVar[dict]
    unit_value: float
    stack: Dict[str, Position] = field(init=False, default_factory=dict)

    def __post_init__(self, unit_params: dict):
        for symbol, param in unit_params.items():
            self.stack[symbol] = Position(symbol, param)

    def parse_open_positions(self, open_positions: dict):
        before = {}
        pos_updates = ph.formatted_positions_update(open_positions, self.open())
        for symbol, pos_update in pos_updates.items():
            if symbol not in self.stack:
                self.add_position(symbol)
            before[symbol] = self.stack[symbol].parse_update_dict(pos_update)
        return before

    def parse_attributes(self, key: str, attributes: dict):
        for symbol, value in attributes.items():
            self.stack[symbol].parse_update_dict({key: value})

    def add_position(self, symbol, param=None):
        if param is None:
            param = ph.get_unit_param(None, symbol, self.unit_value)
        self.stack[symbol] = Position(symbol, param)

    def return_list(self):
        return list(self.stack.values())

    def get_dict(self, symbol):
        return self.stack[symbol].return_dict()

    def open(self, return_copy_dict=False):
        open =  ph.find_open_positions(self.return_list())
        if return_copy_dict:
            return ph.pos_list_to_dict(open, as_objects=False)
        return ph.pos_list_to_dict(open, as_objects=True)





