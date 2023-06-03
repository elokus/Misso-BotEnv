import ccxt, logging, time, asyncio, queue, json, os, threading
import Misso.services.logger as log
import Misso.services.helper as mh
import Misso.services.async_helper as ah
import Misso.services.cust_decorator as decor
from Misso.market_states import get_markets_states, marketWatcher
from Misso.models.position import Position

class SubManager:
    def __init__(self, subaccount: str, **kwargs):
        self.name = str(__class__.__name__) + "_" + subaccount
        self.subaccount = subaccount
        
        self.exchange: ccxt = None
        self.logger = None
        self.timeframes = ["1m", "15m", "1h", "4h"]
        self.slot_map = {"buy": 5, "sell": 5, "both": 8}
        self.out_msg = None
        self.in_msg = None
        self.watch_list = None
        self.restricted = []
        self.market_review = None
        self.storage_dir = None
        self.restore_manager = False
        self.open_position = []
        self.risk_positions = set()
        self.new_risk_positions = set()
        self.last_pos_update = 0

        ## Parameter
        self.BASE_VALUE = 1
        self.BASE_TIMEFRAME = "1m"
        self.TARGET_BY = "next"
        self.TARGET_FACTOR = 0.5
        self.RISK_VALUE = 30

        self.init_attributes(**kwargs)

        self.slots = queue.Queue()
        self.pos = {}
        self.active_watcher = {}
        self.active_markets = []
        self.blocked_slots = {"buy":0, "sell":0, "both":0}

        self.init_markets()
        self.init_positions()
        self.get_open_slots()

    def init_attributes(self, **kwargs):
        if len(kwargs) > 0:
            for attr, arg in kwargs.items():
                setattr(self, attr, arg)

            if "config" in kwargs:
                for key, value in kwargs["config"].items():
                    setattr(self, key, value)
        self.restricted = set(self.restricted)

    def init_markets(self):
        if self.watch_list is None:
            if self.market_review is None:
                self.watch_list = mh.execute_asyncio(ah.get_filtered_watch_list, None, None)
            else:
                self.watch_list = list(self.market_review.keys())

        if self.market_review is None:
            self.market_review = marketWatcher.get_states(self.watch_list)
        self._init_clean_markets(min_timeframe="1h")
        self.log(f"marketWatcher and watch_list with {len(self.watch_list)} markets in market_review")

    def init_positions(self):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        if self.restore_manager:
            restore_data = mh.get_latest_json(f"{self.subaccount}_states", dir=f"{self.storage_dir}\\restore")
            if restore_data is None:
                self.pos = Position.init_positions(self.exchange, self.watch_list, self.BASE_VALUE, market_review=self.market_review, base_timeframe=self.BASE_TIMEFRAME)
            else:
                self.pos = Position.restore_positions(self.exchange, restore_data, self.watch_list, self.BASE_VALUE, market_review=self.market_review, base_timeframe=self.BASE_TIMEFRAME)
                self._init_clean_position()
        else:
            self.pos = Position.init_positions(self.exchange, self.watch_list, self.BASE_VALUE, market_review=self.market_review, base_timeframe=self.BASE_TIMEFRAME)
        self.init_open_positions()


    def init_open_positions(self):
        self.clean_open_position()
        if not self.restore_manager:
            for symbol, pos in self.pos.items():
                if pos.is_open:
                    self.log(f"Init open Position: {symbol}")
                    self.exchange.cancel_all_orders(symbol=symbol)
                    pos.set_closing_order(self.exchange, profit_target=0.003, logger=self.logger)
                    pos.place_next_dca_order(self.exchange, logger=self.logger)

    def get_open_slots(self):
        self.get_blocked_slots()
        slots = [k for k, v in self.slot_map.items() for i in range(v)]
        self.log(f"open_slots: {slots}")
        self.log(f"blocked_slots: {self.blocked_slots}")
        for slot in slots:
            if self.blocked_slots[slot] <= 0:
                self.slots.put(slot)
            else:
                self.blocked_slots[slot] -= 1

    def get_blocked_slots(self):
        open_pos = self.exchange.fetch_positions()
        long, short = 0, 0
        for pos in open_pos:
            if pos["contracts"] > 0:
                if pos["side"] == "long":
                    long += 1
                if pos["side"] == "short":
                    short += 1
        self.blocked_slots["buy"] = long
        self.blocked_slots["sell"] = short

    def run(self):
        try:
            self.open_markets()
            self.message_processing()
        except Exception as e:
            self.log(f"Error in run(): {e}")

    def open_markets(self):
        if self.slots.empty():
            return
        buy_rank, sell_rank = marketWatcher.get_filtered_rr_ranking(self.market_review, self.BASE_TIMEFRAME, skip=self.active_markets)
        buy_rank = [m for m in buy_rank.keys() if m not in self.restricted]
        sell_rank = [m for m in sell_rank.keys() if m not in self.restricted]
        _buys, _sells = [], []
        while not self.slots.empty():
            slot = self.slots.get()
            if (slot == "buy" or slot == "both") and len(buy_rank) > 0:
                new = buy_rank.pop(0)
                self.active_markets.append(new)
                self.place_orders(new, "buy")
            if (slot == "sell" or slot == "both") and len(sell_rank) > 0:
                new = sell_rank.pop(0)
                self.active_markets.append(new)
                self.place_orders(new, "sell")

    def message_processing(self):
        while not self.in_msg.empty():
            msg = self.in_msg.get()
            self.log(f"has new watcher Message: {msg}")
            self._message_processing(msg)

    def add_to_watcher(self, pos: Position):
        if pos.symbol not in self.active_watcher:
            self.active_watcher[pos.symbol] = {"tx":pos.current_timeframe, "mr":pos.current_market_state["meta_range"]}
            msg = ("watcher",{self.subaccount:{pos.symbol:self.active_watcher[pos.symbol]}})
            self.log(f"putting msg: {msg}")
            self.out_msg.put(msg)
        else:
            if not self.active_watcher[pos.symbol]["tx"] == pos.current_timeframe:
                self.active_watcher[pos.symbol] = {"tx":pos.current_timeframe, "mr":pos.current_market_state["meta_range"]}
                msg = ("watcher",{self.subaccount:{pos.symbol:self.active_watcher[pos.symbol]}})
                self.log(f"putting msg: {msg}")
                self.out_msg.put(msg)

    def remove_from_watcher(self, pos):
        msg = ("watcher", {self.subaccount:{pos.symbol:"remove"}})
        self.log(f"putting msg: {msg}")
        self.out_msg.put(msg)

    def place_orders(self, market: str, side: str):
        self.pos[market].update_current_wave_pairs(target_by=self.TARGET_BY)
        self.pos[market].create_orders_by_side(self.exchange, side, target_factor=self.TARGET_FACTOR, place_orders=True, logger=self.logger)
        self.add_to_watcher(self.pos[market])







    ### UTILITIES

    @decor.safe_request
    def _fetch_open_positions(self):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        _pos = self.exchange.fetch_positions()
        return [p for p in _pos if p["contracts"] > 0]

    @decor.safe_request
    def _fetch_open_orders(self):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        return self.exchange.fetch_open_orders()

    def _message_processing(self, msg):
        if msg[1] == "STOP":
            self.stop_routine()

        elif msg[1] == "CLEAN":
            self.clean_slots()

        elif len(msg) < 2:
            return

        elif msg[2] == "out_of_range":
            self.out_of_range(msg)

        elif msg[2] == "exit_order_closed":
            self.order_closed(msg)




    def clean_slots(self):
        self.log("cleaning slots")
        self.clean_open_position()
        self.active_markets = []
        for s, pos in self.pos.items():
            if pos.is_open:
                self.active_markets.append(s)
            else:
                self.exchange.cancel_all_orders(symbol=s)
        self.clean_open_orders()
        self.get_open_slots()

    def clean_open_orders(self):
        open_orders = self._fetch_open_orders()
        to_cancel = []
        tmp_order = []
        for o in open_orders:
            _o = (o["symbol"], o["amount"], o["side"], o["price"])
            if _o in tmp_order:
                to_cancel.append(o["id"])
            tmp_order.append(_o)
        for id in to_cancel:
            try:
                self.exchange.cancel_order(id)
            except:
                continue


    def clean_open_position(self):
        if time.time() - 60 < self.last_pos_update:
            return

        for s, pos in self.pos.items():
            pos.is_open = False

        open_pos = self._fetch_open_positions()
        open_syms = [p["symbol"] for p in open_pos]
        self.log(f"clean opos found {len(open_syms)} positions: {open_syms}")
        for p in open_pos:
            if not p["symbol"] in self.pos:
                self.pos[p["symbol"]] = Position.from_order_value(self.exchange, sub=self.subaccount, symbol=p["symbol"], base_order_value=self.BASE_VALUE, base_timeframe=self.BASE_TIMEFRAME)
                self.pos[p["symbol"]].parse_current_market_states(marketWatcher.get_states([p["symbol"]]), self.BASE_TIMEFRAME)
                self.pos[p["symbol"]].update_current_wave_pairs()
            self.pos[p["symbol"]].is_open = True
            self.pos[p["symbol"]].update(p)
            self.pos[p["symbol"]].check_risk_status(self.RISK_VALUE)
            if self.pos[p["symbol"]].is_risk and p["symbol"] not in self.risk_positions:
                self.new_risk_positions.add(p["symbol"])
            else:
                self.risk_positions.discard(p["symbol"])
        self.last_pos_update = time.time()

    def handle_risk_positions(self):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        for sym in self.new_risk_positions:
            self.log(f"NEW RISK POSITION: {sym}")
            self.exchange.cancel_all_orders()
            self.pos[sym].place_next_dca_order(self.exchange, logger=self.logger)
            self.pos[sym].set_closing_order(self.exchange, profit_target=0.003, logger=self.logger)
            self.pos[sym].reset_timeframe()
            self.pos[sym].parse_current_market_states(self.market_review)
            self.place_orders(sym, "both")

        self.new_risk_positions = set()

    def order_closed(self, msg):
        symbol = msg[1]
        exit_order = msg[3]
        res = self.pos[symbol].parse_exit_order(self.exchange, exit_order)
        if res is not None:
            self.log(f"replacing OrderPair for {symbol} at {res}")

    def out_of_range(self, msg):
        self.clean_open_position()
        symbol = msg[1]
        range_break = msg[3]
        if msg[3]["tx"] == self.pos[symbol].current_timeframe:
            if self.pos[symbol].is_open:
                if self.pos[symbol].side == range_break["side"]:
                    self.exchange.cancel_all_orders(symbol=symbol)
                    self.pos[symbol].set_closing_order(self.exchange, profit_target=0.003, logger=self.logger)
                    self.pos[symbol].next_timeframe(self.exchange, timeframes=self.timeframes)
                    self.log(f"BROKEN RANGE for {symbol} new timeframe: {self.pos[symbol].current_timeframe}")
                    self.add_to_watcher(self.pos[symbol])
                    self.place_orders(symbol, range_break["side"])
            else:
                self.log(f"BROKEN RANGE for {symbol} but not open Position cancelling remaining orders")
                self.exchange.cancel_all_orders(symbol=symbol)
                self.remove_from_watcher(self.pos[symbol])
                if symbol in self.active_markets:
                    self.active_markets.remove(symbol)
                self.pos[symbol].parse_current_market_states(self.market_review)


    def _init_clean_markets(self, min_timeframe="1h"):
        market_review = self.market_review.copy()
        for market, states in market_review.items():
            if min_timeframe not in states:
                self.restricted.add(market)

    def _init_clean_position(self):
        for market, pos in self.pos.items():
            if pos.is_open:
                if len(pos.current_market_state) == 0:
                    pos.fetch_current_market_state(self.exchange)
                self.log(f"restored position for {market}")
                self.active_markets.append(market)
                self.add_to_watcher(pos)
            else:
                self.exchange.cancel_all_orders(market)

    def log(self, msg):
        self.logger.info(f"{self.name}: {msg}")

    def stop_routine(self):
        pass


    @classmethod
    def init_from_watch_list(cls, watch_list, subaccount, exchange=None, filter_func=None, filter_args=[], filter_kwargs={}, timeframes = ["1m", "15m", "1h", "4h"], **kwargs):
        if exchange is None:
            exchange = mh.initialize_exchange_driver("Main")
            exchange = mh.change_sub(exchange, subaccount)
        if filter_func is not None:
            watch_list = filter_func(*filter_args, **filter_kwargs)
        market_review = get_markets_states(watch_list, timeframes)
        return cls(subaccount, exchange, watch_list=watch_list, market_review=market_review, **kwargs)

    @classmethod
    def init_from_market_review(cls, market_review_path: str, subaccount: str, **kwargs):
        if "exchange" not in kwargs:
            exchange = mh.initialize_exchange_driver("Main")
            exchange.change_sub(subaccount)
        if isinstance(market_review_path, dict):
            ms = market_review_path
        else:
            ms = mh.load_from_json(market_review_path)
        return cls(subaccount, exchange, market_review=ms, **kwargs)


# class SubCounter:
#     def __init__(self):




