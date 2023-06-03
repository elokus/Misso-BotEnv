import time
import ccxt
from dataclasses import dataclass, field
from Misso.services.helper import parse_side, parse_exit_side
import Misso.services.helper as mh
from Misso.models.draft.swing_executor.orders import Order as orders

def init_positions(exchange: ccxt, symbols: str, base_value: float, base_timeframe: str="1m", market_states:dict=None,  **kwargs):
    d ={}
    for s in symbols:
        d[s] = init_position(exchange, s, base_value, base_timeframe=base_timeframe, **kwargs)
        if not market_states is None:
            d[s].parse_current_market_states(market_states, base_timeframe)
            d[s].update_current_wave_pairs()
    return d

def init_position(exchange: ccxt, symbol: str, base_order_value:float = 1, **kwargs):
    m = exchange.market(symbol)
    sub = exchange.headers['FTX-SUBACCOUNT']
    size = base_order_value/float(m["info"]["last"]) if base_order_value/float(m["info"]["last"]) > m["limits"]["amount"]["min"] else m["limits"]["amount"]["min"]
    size = float(exchange.amount_to_precision(symbol, size))
    return Position(symbol, base_order_size=size, subaccount=sub, **kwargs) if kwargs else Position(symbol, subaccount=sub, base_order_size=size)

def filter_positions(positions: dict, cond_kwargs):
    filtered = []
    for s, p in positions:
        cond = True
        for attr, arg in cond_kwargs:
            if getattr(p, attr) != arg:
                cond = False
        if cond:
            filtered.append((s, p))
    return filtered

@dataclass
class Position:
    symbol: str
    subaccount: str = None
    base_timeframe: str = 0.0
    base_time_index: int = 0
    base_order_size: float = 0.0
    side: str = None
    size: float = 0.0
    notional: float = 0.0
    unrealized: float = 0.0
    break_even: float = 0.0
    trade_count: int = 0
    dca_count: int = 0

    current_market_state: dict = field(default_factory=dict)
    current_timeframe: str = "1m"
    current_time_level: int = 0
    current_base_size: float = 0.0
    current_order_pairs: dict = field(default_factory=dict)
    current_wave_pairs: dict = field(default_factory=dict)
    closed_wave_pairs: dict = field(default_factory= dict)
    is_open: bool = False
    is_pending: bool = False
    has_fills: bool = False
    is_priority: bool = False

    def __post_init__(self):
        self.base_time_index = mh.enumerate_timeframes(self.base_timeframe)
        self.current_timeframe = self.base_timeframe
        self.current_time_level = 0
        self.current_base_size = self.base_order_size

    @classmethod
    def init_from_open_pos(cls, open_pos):
        return cls(open_pos["symbol"])

    def update(self, response):
        _side = self.side
        _size = self.size
        self.size = response["contracts"]
        self.side = parse_side[response["side"]] if self.size > 0 else self.side
        self.notional = response["notional"]
        self.unrealized = response["unrealizedPnl"]
        self.set_break_even(response)

    def next_timeframe(self, exchange: ccxt, force_timeframe: str=None, force_size_factor: float=None):
        self.current_time_level += 1
        self.current_timeframe = mh.shift_timeframe(self.current_timeframe, 1) if not force_timeframe else force_timeframe
        self.current_base_size = self.base_order_size + self.current_time_level*10*self.base_order_size if not force_size_factor else force_size_factor
        self.fetch_current_market_state(exchange)

    def fetch_current_market_state(self, exchange: ccxt=None, timeframe: str=None, next_timeframe: bool = False):
        if next_timeframe:
            self.next_timeframe(force_timeframe=timeframe)
        from Misso.market_states import get_market_state
        timeframe = self.current_timeframe if not timeframe else timeframe
        market_state = get_market_state(self.symbol, timeframe, exchange)
        self.parse_current_market_states(market_state, timeframe)

    def get_wave_pairs(self, by="next"):
        return mh.get_wave_target_pairs(self.current_market_state, self.current_timeframe, by=by)

    def update_current_wave_pairs(self, target_by="next"):
        self.current_wave_pairs = self.get_wave_pairs(by=target_by)

    def place_wave_pair_orders(self, exchange: ccxt, side: str="both", target_factor: float=0.5):
        sides = ["buy", "sell"] if side == "both" else [side]
        _client_ids = []
        for s in sides:
            self.current_order_pairs = orders.create_wave_orders_by_position(exchange, self, s, target_factor=target_factor)
            for idx, op in self.current_order_pairs[s].items():
                for o in op:
                    o.create(exchange)
                    _client_ids.append(o.client_id)
                time.sleep(0.2)
        return _client_ids

    def parse_current_market_states(self, market_states:dict, timeframe: str=None):
        timeframe = self.current_timeframe if not timeframe else timeframe
        ms = market_states[self.symbol] if self.symbol in market_states else market_states
        if timeframe in ms:
            self.current_market_state = ms[timeframe].copy()
        else:
            self.current_market_state = ms.copy()

    def parse_closed_exit_order(self, exit_order: dict):
        side, idx = self.get_order_pair_idx(exit_order)
        if self.current_timeframe not in self.closed_wave_pairs:
            self.closed_wave_pairs[self.current_timeframe] = {"sell":{},"buy":{}}
        self.closed_wave_pairs[self.current_timeframe][side][idx] = self.current_wave_pairs[self.current_timeframe][side][idx]

    def get_order_pair_idx(self, exit_order: dict):
        side = mh.parse_exit_side(exit_order["side"])
        for idx, wp in self.current_order_pairs[side].items():
            if wp[1]["amount"] == exit_order["amount"]:
                return side, idx
        return None, None



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
