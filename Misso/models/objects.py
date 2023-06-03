from dataclasses import dataclass, field

@dataclass
class Position:
    symbol: str
    subaccount: str = None
    id: str = None
    is_open: bool = False
    is_active: bool = False

    side: str = None
    size: float = 0.0
    notional: float = 0.0
    unrealized: float = 0.0
    break_even: float = 0.0
    timeframes: list = None
    size_increment: int = 4

    base_tx: str = "1m"
    base_txi: int = 0
    base_size: float = 0.0
    curr_tx: str = "1m"
    curr_txi: int = 0
    curr_size: float = 0.0
    curr_waves: dict = field(default_factory=dict)
    curr_range: list = None
    curr_order_pairs: dict = field(default_factory=dict)
    curr_wave_pairs: dict = field(default_factory=dict)
    last_range: list = None
    order_id_map: dict = field(default_factory=dict)
    slot: str = None
    breakout: str = None
    has_breakout: bool = False
    has_closed_orders: bool = False
    _closed_op: list = field(default_factory=list)

    def update_live(self, resp):
        pass

    def export_live(self):
        pass

    def reset_timeframe(self):
        pass

    def update_wave_range(self, review: dict):
        pass

    def update_wave_pairs(self, target_by: str):
        pass

    def place_order_pairs(self, exchange: object, slot: str, logger: object):
        pass

    def get_wave_pairs(self, target_by: str):
        pass

    def create_orders(self):
        pass

    def upgrade_timeframe(self):
        pass

    def downgrade_timeframe(self):
        pass

    def parse_closed_order(self):
        pass

    def cancel_order_pairs(self, exchange: object):
        pass

    def replace_order_pair(self, exchange: object, idx: float, tx: str, logger: object):
        pass

    @property
    def has_open_ops(self):
        return

@dataclass
class Order:
    size: float = None
    price: float = None
    side: str = None
    symbol: str = None
    params: dict = None
    id: str = None
    client_id: str = None
    status: str = "generated"
    filled: float = 0.0
    _filled: float = 0.0
    trigger_price: float = None
    remaining: float = None
    value: float = None
    opened_at: float = None
    index: int = None
    is_conditional = False
    closed_at: int = None
    is_placed: bool = False
    is_open: bool = False
    is_closed: bool = False
    is_canceled: bool = False
    is_processed: bool = False
    is_partially_filled: bool = False
    is_updated: bool = False