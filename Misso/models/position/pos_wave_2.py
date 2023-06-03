from dataclasses import dataclass, field
import ccxt
import Misso.services.utilities as utils
from Misso.models.position import PosLive
from Misso.models.order import Order


@dataclass
class PosWave(PosLive):

    timeframes: list = None
    size_increment: int = 4

    base_tx: str = "1m"
    base_txi: int = 0
    base_value: float = 0.0
    base_size: float = 0.0

    curr_tx: str = "1m"
    curr_txi: int = 0
    curr_size: float = 0.0

    curr_waves: dict = field(default_factory=dict)
    curr_range: list = None

    orders: dict = field(default_factory=dict)
    _orders: list = field(default_factory=list)

    curr_order_pairs: dict = field(default_factory=dict)
    curr_wave_pairs: dict = field(default_factory=dict)
    _closed_op: list = field(default_factory=list)

    has_breakout: bool = False
    has_closed_op: bool = False
    is_risk: bool = False

    last_range: list = None
    order_id_map: dict = field(default_factory=dict)
    slot: str = None
    breakout: str = None


    def __post_init__(self):
        super().__post_init__()
        #self.reset_timeframe()

    def reset(self, exchange: ccxt):
        self.reset_timeframe()
        self.reset_live()
        self.reset_orders(exchange)
        self.is_active = False
        self.slot = None

    def reset_timeframe(self):
        self.curr_tx = self.base_tx
        self.curr_txi = 0
        self.curr_size = self.base_size

    def reset_orders(self, exchange: ccxt):
        exchange.cancel_all_orders(self.symbol)
        self.curr_order_pairs = {}
        self.orders = {}
        self.order_id_map = {}


    def update_wave_range(self, reviews: dict):
        review = reviews[self.symbol]
        self.curr_waves = review[self.curr_tx]["waves"]
        self.curr_range = review[self.curr_tx]["meta_range"]

    def update_wave_pairs(self, target_by: str, target_factor: float):
        self.curr_wave_pairs = self.get_wave_pairs(target_by, target_factor)

    def place_order_pairs(self, exchange: ccxt, side: str, logger: object):
        last = utils.safe_last_price(exchange, self.symbol)
        for idx in self._get_wp_by_side(side):
            self.place_order_pair(exchange, side, idx, logger, last)


    def place_order_pair(self, exchange, side, idx, logger, last=None):
        wp = self.curr_wave_pairs[int(idx)]
        last = utils.safe_last_price(exchange, self.symbol) if last is None else last
        if utils.is_valid_wave_pair(side, last, idx, wp):
            entry_id = self._place_entry_op(exchange, idx, wp, logger)
            if entry_id is None:
                return
            exit_id = self._place_exit_op(exchange, idx, wp, logger, close_pos=self._should_close(entry_id, wp[1]))
            self.curr_order_pairs[f"{idx}_{self.curr_tx}"] = (entry_id, exit_id)


    def _place_entry_op(self, exchange, idx, wp, logger):
        op_id = f"{idx}_{self.curr_tx}"
        side = "buy" if idx < 0 else "sell"
        size = self.curr_size * abs(idx)
        price = wp[0]
        entry = Order(size, price, side, self.symbol)
        if entry.create(exchange, logger=logger):
            self.orders[entry.id] = entry
            self.order_id_map[entry.id] = op_id
            self._orders.append(entry.id)
            return entry.id
        del entry
        return None

    def _place_exit_op(self, exchange, idx, wp, logger, close_pos=False):
        op_id = f"{idx}_{self.curr_tx}"
        size = self.curr_size * abs(idx) if not close_pos else self.curr_size * abs(idx) + self.size
        price = wp[1]
        side = "sell" if idx < 0 else "buy"
        params = {"triggerPrice":wp[0]}
        exit = Order(size, price, side, self.symbol, params=params)
        if exit.create(exchange, logger=logger):
            self.orders[exit.id] = exit
            self.order_id_map[exit.id] = op_id
            self._orders.append(exit.id)
            return exit.id
        del exit
        return None


    def get_wave_pairs(self, target_by: str, target_factor: float):
        wps = utils.get_wave_pairs(self.curr_waves, target_by)
        return utils.scale_wave_pairs(wps, target_factor)

    def upgrade_timeframe(self):
        self.curr_txi += 1
        self.curr_tx = utils.timeframe_by_index(self.curr_txi, self.timeframes)
        self.curr_size = (self.curr_txi+1) * self.base_size
        self.last_range = self.curr_range

    def downgrade_timeframe(self):
        self.curr_txi -= 1
        if self.curr_txi > 0:
            self.curr_tx = utils.timeframe_by_index(self.curr_txi, self.timeframes)
            self.curr_size = (self.curr_txi+1) * self.base_size
        if self.curr_txi <= 0:
            self.reset_timeframe()


    def parse_closed_order(self, msg: str):
        oid = msg
        if oid in self.orders:
            self.orders[oid].is_closed = True
            op_id = self.order_id_map[oid]
            entry_id, exit_id = self.curr_order_pairs[op_id]
            if entry_id != oid:
                if entry_id is None or self.orders[entry_id].is_closed:
                    self.close_order_pair(op_id)
            if exit_id != oid:
                if exit_id is None or self.orders[exit_id].is_closed:
                    self.close_order_pair(op_id)

    def replace_order_pair(self,exchange: ccxt, idx: float, tx: str, logger: object):
        if tx == self.curr_tx:
            self.place_order_pair(exchange, self.slot, idx, logger)

    @property
    def has_open_ops(self):
        if len(self.curr_order_pairs) > 0:
            for op, oids in self.curr_order_pairs.items():
                if self.orders[oids[0]].is_closed and not self.orders[oids[1]].is_closed:
                    return True
                if self.orders[oids[1]].is_closed and not self.orders[oids[0]].is_closed:
                    return True
        return False

    def close_order_pair(self, order_pair_id: str):
        if order_pair_id in self.curr_order_pairs:
            self._closed_op.append(order_pair_id)
            self._remove_order_pair(order_pair_id)
            self.has_closed_op = True

    def cancel_order_pairs(self, exchange: ccxt):
        for opid in self._get_opid_iter():
            self.cancel_order_pair(exchange, opid)

    def cancel_order_pair(self, exchange: ccxt, opid: str):
        for oid in self._get_oid_iter(opid):
            utils.safe_cancel_order(exchange, oid)
        self._remove_order_pair(opid)

    def _remove_order_pair(self, opid: str):
        if opid in self.curr_order_pairs:
            oids = self.curr_order_pairs.pop(opid)
            for oid in oids:
                self._remove_order(oid)

    def _remove_order(self, oid: str):
        if oid in self.orders:
            del self.orders[oid]
        if oid in self.order_id_map:
            del self.order_id_map[oid]

    def _get_oid_iter(self, order_pair_id: str):
        if order_pair_id in self.curr_order_pairs:
            return iter(self.curr_order_pairs[order_pair_id])
        return None

    def _get_opid_iter(self):
        return iter(self.curr_order_pairs.copy())

    def _get_wp_by_side(self, side: str):
        if side == "buy":
            return {k: v for k, v in self.curr_wave_pairs.items() if k < 0}
        if side == "sell":
            return {k: v for k, v in self.curr_wave_pairs.items() if k > 0}
        else:
            return self.curr_wave_pairs

    def close_and_reset(self, exchange: ccxt):
        self.market_close(exchange)
        self.reset(exchange)

    def market_close(self, exchange: ccxt):
        side = "buy" if self.side == "sell" else "sell"
        utils.market_close_position(exchange, self.symbol, side, self.size)

    def _should_close(self, entry_id: str, exit_price: float):
        if self.curr_txi > 0 and self.is_open:
            return self._break_even_in_op_range(self.orders[entry_id], exit_price)
        return False


    def _break_even_after_order(self, order: Order):
        if self.is_open and self.side == order.side:
            _notional = self.notional + order.value
            _size = self.size + order.size
            return _notional / _size
        return None

    def _break_even_in_op_range(self, entry_order: Order, exit_price: float):
        _be = self._break_even_after_order(entry_order)
        if _be is not None:
            return exit_price >= _be if self.side == "buy" else exit_price <= _be
        return False

