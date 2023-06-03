import ccxt
from dataclasses import dataclass, field
import Misso.services.utilities as utils
import Misso.services.helper as mh
from Misso.models import Order, CustomId
from Misso.models.draft.order_pair import OrderPair


@dataclass
class Position:
    symbol: str
    subaccount: str = None

    #live-data
    side: str = None
    size: float = 0.0
    notional: float = 0.0
    unrealized: float = 0.0
    break_even: float = 0.0

    # Level-Cluster-Strategy specific
    base_timeframe: str = "1m"
    base_time_index: int = 0
    base_order_size: float = 0.0
    size_level_factor: int = 4

    #current_market_state: dict = field(default_factory=dict)
    current_buy_waves: list = None
    current_sell_waves: list = None


    current_timeframe: str = "1m"
    current_range: list = None
    current_time_level: int = 0
    current_base_size: float = 0.0
    current_order_pairs: dict = field(default_factory=dict)
    current_wave_pairs: dict = field(default_factory=dict)
    closing_order: Order = None
    dca_order: Order = None

    slot: str = None
    is_open: bool = False
    is_risk: bool = False
    ignore_opos: bool = False

    def __post_init__(self):
        self.base_time_index = utils.index_by_timeframe(self.base_timeframe)
        self.current_timeframe = self.base_timeframe
        self.current_time_level = 0
        self.current_base_size = self.base_order_size

    def reset(self):
        self.slot = None
        self.is_open = False
        self.is_risk = False
        self.reset_timeframe()

    def reset_timeframe(self):
        self.current_time_level = 0
        self.current_timeframe = self.base_timeframe
        self.current_time_level = 0
        self.current_base_size = self.base_order_size

    def parse_review(self, market_review: dict):
        if self.current_timeframe in market_review:
            self.current_range = market_review[self.current_timeframe]["meta_range"]
            self.update_waves(market_review[self.current_timeframe])
        else:
            print(f"Pos({self.symbol}): timeframe {self.current_timeframe} not in market_review")

    def update_waves(self, market_review_tx:dict):
        self.current_buy_waves = market_review_tx[self.current_timeframe]["data"]["low_waves"]
        self.current_sell_waves = market_review_tx[self.current_timeframe]["data"]["high_waves"]
        self.current_wave_pairs = utils.get_wave_target_pairs(self.current_buy_waves, self.current_sell_waves)

    def create_orders_by_side(self, exchange: ccxt, side: str, target_factor:float=1, logger=None):
        last = mh.safe_last_price(exchange, self.symbol)
        for idx, wp in self.current_wave_pairs[self.current_timeframe][side].items():
            entry, exit = wp[0], wp[1]
            if (side=="buy" and entry > last) or (side=="sell" and entry < last):
                continue
            if side not in self.current_order_pairs:
                self.current_order_pairs[side] = {}
            self.current_order_pairs[side][idx] = OrderPair.from_side_idx(self, side, idx, target_factor)
            self.current_order_pairs[side][idx].place_orders(exchange, logger=logger)





    def get_slot_type_by_open_orders(self, open_orders: list):
        ds = set()
        for o in open_orders:
            cid = o["clientOrderId"]
            if cid is not None:
                try:
                    spez = cid.split("_")[-1]
                    if spez.split("%")[1] == "entry":
                        ds.add(o["side"])
                except:
                    continue
        if "sell" in ds and "buy" in ds:
            self.slot = "both"
        elif "sell" in ds:
            self.slot = "sell"
        elif "buy" in ds:
            self.slot = "buy"

    def get_timeframe_by_size(self):
        if self.size > 0:
            for i in range(4):
                if self.size / 5 < self.base_order_size + (self.base_order_size * self.size_level_factor * i):
                    return utils.timeframe_by_index(i)
            return "4h"
        return None


    def next_timeframe(self, exchange: ccxt, force_timeframe: str=None, force_size_factor: float=None, timeframes: list=None):
        self.current_time_level += 1
        if timeframes is None:
            self.current_timeframe = mh.shift_timeframe(self.current_timeframe, 1) if not force_timeframe else force_timeframe
        else:
            self.current_timeframe = timeframes[self.current_time_level] if not force_timeframe else force_timeframe
        self.current_base_size = self.base_order_size + self.current_time_level*self.size_level_factor*self.base_order_size if not force_size_factor else force_size_factor
        self.fetch_current_market_state(exchange)


        timeframe = self.current_timeframe if not timeframe else timeframe
        ms = market_states[self.symbol] if self.symbol in market_states else market_states
        if timeframe in ms:
            self.current_market_state = ms[timeframe].copy()
        else:
            self.current_market_state = ms.copy()
        self.current_range = self.current_market_state["meta_range"]
















    def fetch_current_market_state(self, market_streamer, timeframe: str=None, next_timeframe: bool = False):
        if next_timeframe:
            self.next_timeframe(force_timeframe=timeframe)
        from Misso.market_states import get_market_state
        timeframe = self.current_timeframe if not timeframe else timeframe
        market_state = get_market_state(self.symbol, timeframe, exchange)
        self.parse_current_market_states(market_state, timeframe)



    def place_current_order_pairs(self, exchange):
        for side, ops in self.current_order_pairs.items():
            for idx, op in ops.items():
                op.place_orders(exchange)

    def get_wave_pairs(self, by="next"):
        return mh.get_wave_target_pairs(self.current_market_state, self.current_timeframe, by=by)

    def update_current_wave_pairs(self, target_by="next"):
        self.current_wave_pairs = self.get_wave_pairs(by=target_by)

    def parse_current_market_states(self, market_states:dict, timeframe: str=None):
        timeframe = self.current_timeframe if not timeframe else timeframe
        ms = market_states[self.symbol] if self.symbol in market_states else market_states
        if timeframe in ms:
            self.current_market_state = ms[timeframe].copy()
        else:
            self.current_market_state = ms.copy()
        self.current_range = self.current_market_state["meta_range"]

    def parse_exit_order(self, exchange: ccxt, exit_order: dict):
        side = mh.parse_exit_side[exit_order["side"]]
        if side in self.current_order_pairs:
            for idx, op in self.current_order_pairs[side].items():
                exit_price = float(exit_order["info"]["orderPrice"]) if "orderPrice" in exit_order["info"] else exit_order["price"]
                if op.exit_size == exit_order["amount"] and (op.exit_price > exit_price*0.995 and op.exit_price < exit_price*1.005):
                    op.close_pair()
                    op.place_orders(exchange)
                    return [side, idx]
        return None

    ###########################
    ###   DCA-EXIT MODUL


    def get_break_even_range(self, ms, timeframes=["1m", "15m", "1h", "4h"]):
        for tx in timeframes:
            if tx in ms:
                if mh.price_in_range(self.break_even, ms[tx]["meta_range"]):
                    return tx, ms[tx]["meta_range"]
        tx = "4h" if "4h" in ms else "1h"
        _range = [ms[tx]["meta_range"][0]*0.8, ms[tx]["meta_range"][0]*1.2]
        return tx, _range

    def place_next_dca_order(self, exchange: ccxt, max_value: float=100, logger=None):
        from Misso.market_states import get_market_state
        subaccount = exchange.header["FTX-SUBACCOUNT"] if self.subaccount is None else self.subaccount

        ms = get_market_state(self.symbol)
        tx, be_range = self.get_break_even_range(ms)
        logger.info(f"{subaccount} position.place_next_dca: for {self.symbol} break_even range is {be_range} in timeframe {tx}")
        dca_price = float(be_range[0]) if self.side == "buy" else float(be_range[1])
        dca_size = self.size
        logger.info(f"{subaccount} position.place_next_dca: {self.symbol} (size={dca_size}, price={dca_price})")
        if dca_price * dca_size > max_value:
            dca_size = max_value/dca_price

        self.dca_order = Order(dca_size, dca_price, self.side, self.symbol, CustomId.generate_params(subaccount, self.symbol, type="DCA"))
        self.dca_order.create(exchange, logger)

    ###########
    ### BASEMETHODS

    def update(self, response):
        self.size = float(response["contracts"])
        self.notional = response["notional"]
        self.unrealized = response["unrealizedPnl"]
        self.side = mh.safe_parse_side(response)
        self.break_even = mh.safe_parse_break_even(response)

    def check_risk_status(self, limit):
        if self.notional >= limit:
            self.is_risk = True
        else:
            self.is_risk = False

    def set_closing_order(self, exchange: ccxt, profit_target: float=0.003, logger=None):
        self.closing_order = Order.get_exit(self, profit_target)
        self.closing_order.create(exchange, logger=logger)





    ###########
    ### CLASSMETHODS

    @classmethod
    def init_from_open_pos(cls, open_pos):
        return cls(open_pos["symbol"])

    @classmethod
    def from_order_value(cls, exchange: ccxt, sub:str, symbol: str, base_order_value: float=1, **kwargs):
        m = exchange.market(symbol)
        size = base_order_value/float(m["info"]["last"]) if base_order_value/float(m["info"]["last"]) > float(m["info"]["minProvideSize"]) else float(m["info"]["minProvideSize"])
        size = float(exchange.amount_to_precision(symbol, size))
        return cls(symbol, base_order_size=size, subaccount=sub, **kwargs) if kwargs else cls(symbol, subaccount=sub, base_order_size=size)

    @classmethod
    def from_dict(cls, dict):
        order_pairs = {}
        for side, ops in dict["current_order_pairs"].items():
            order_pairs[side] =  {}
            for idx, op in ops.items():
                order_pairs[side][idx] = OrderPair.from_dict(op)
        dict["current_order_pairs"] = order_pairs
        return cls(**dict)

    ###########
    ### STATICMETHODS

    @staticmethod
    def init_positions(exchange: ccxt, symbols: list, base_value: float, base_timeframe: str="1m", market_states:dict=None,  **kwargs):
        d ={}
        sub = exchange.headers['FTX-SUBACCOUNT']
        for s in symbols:
            d[s] = Position.from_order_value(exchange, sub=sub, symbol=s, base_order_value=base_value, base_timeframe=base_timeframe)
            if not market_states is None:
                d[s].parse_current_market_states(market_states, base_timeframe)
                d[s].update_current_wave_pairs()
        return d

    @staticmethod
    def restore_positions(exchange: ccxt, restore_dict: dict, symbols: str, base_value: float, base_timeframe: str="1m", market_states:dict=None,  **kwargs):
        open_pos = exchange.fetch_positions()
        open_pos = [pos["symbol"] for pos in open_pos if pos["contracts"] > 0]
        watch_list = [sym for sym in symbols if sym not in open_pos]
        d = {}
        for s in open_pos:
            if s in restore_dict:
                d[s] = Position.from_dict(restore_dict[s])
                d[s].is_open = True
        _d = Position.init_positions(exchange, watch_list, base_value, base_timeframe, market_states, **kwargs)
        for s, v in _d.items():
            d[s] = v
        return d

    @staticmethod
    def init_position(exchange: ccxt, symbol: str, base_order_value:float = 1, **kwargs):
        m = exchange.market(symbol)
        sub = exchange.headers['FTX-SUBACCOUNT']
        size = base_order_value/float(m["info"]["last"]) if base_order_value/float(m["info"]["last"]) > m["limits"]["amount"]["min"] else m["limits"]["amount"]["min"]
        size = float(exchange.amount_to_precision(symbol, size))
        return Position(symbol, base_order_size=size, subaccount=sub, **kwargs) if kwargs else Position(symbol, subaccount=sub, base_order_size=size)

    @staticmethod
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