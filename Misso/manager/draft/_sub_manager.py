import ccxt, time, queue
import Misso.services.helper as mh
import Misso.services.cust_decorator as decor
from Misso.models.streamer.reviewer import marketReviewer
from Misso.models.streamer.market_streamer import MarketStreamer
from Misso.models.position import Position
from Misso.models.draft.messaging import MessageHandler

class SubManager(MessageHandler):
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
        self.market_streamer = None
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
        self.MAX_VALUE = 40
        self.TARGET_FACTOR = 0.5
        self.RISK_VALUE = 30
        self.IGNORE_OPOS = True
        self.DEXIT_TARGET = 0.003

        self.init_attributes(**kwargs)
        super().__init__(subaccount, self.in_msg, self.out_msg, self.logger)

        self.slots = queue.Queue()
        self.pos = {}
        self.active_watcher = {}
        self.active_markets = set()

        self.active_buys = set()
        self.active_sells = set()
        self.active_both = set()

        self.blocked_slots = {"buy":0, "sell":0, "both":0}

        self.init_market_streamer()
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

    def init_market_streamer(self):
        if self.market_streamer is None:
            self.market_streamer = MarketStreamer(run_id=self.name, logger=self.logger)
            self.market_streamer.run()
        self.watch_list = self.market_streamer.watch_list
        self.log(f"market_streamer and watch_list with {len(self.watch_list)} markets initialized")

    def init_positions(self):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        if self.restore_manager:
            self.restore_positions()
        else:
            self.generate_positions()
        self.init_open_positions()

    def init_open_positions(self):
        self.clean_open_positions()
        for symbol, pos in self.pos.items():
            if pos.is_open:
                self.init_open_position(pos)

    def init_open_position(self, pos: Position):
        if self.IGNORE_OPOS:
            self.set_dexit(pos, cancel_orders=True)
            pos.reset_timeframe()
            pos.update_waves(self.market_streamer.reviews)
            pos.ignore_opos = True
        else:
            open_orders = self.fetch_open_orders_market(pos.symbol)
            pos.get_slot_type_by_open_orders(open_orders)
            pos.update_waves(self.market_streamer.reviews)
            if pos.slot is None:
                self.set_dexit(pos, cancel_orders=True)
                pos.current_timeframe = pos.get_timeframe_by_size()
            else:
                self.add_to_active_slot(pos)
        self.log(f"Init open Position: {pos.symbol} with timeframe: {pos.current_timeframe} and slot: {pos.slot}")

    def add_to_active_slot(self, pos: Position):
        if pos.slot is None:
            return
        if pos.slot == "buy":
            self.active_buys.add(pos.symbol)
        if pos.slot == "sell":
            self.active_sells.add(pos.symbol)
        if pos.slot == "both":
            self.active_both.add(pos.symbol)

    def generate_positions(self):
        self.pos = Position.init_positions(self.exchange, self.watch_list, self.BASE_VALUE, market_review=self.market_streamer.reviews, base_timeframe=self.BASE_TIMEFRAME)
        self.init_open_positions()

    def restore_positions(self):
        restore_data = mh.get_latest_json(f"{self.subaccount}_states", dir=f"{self.storage_dir}\\restore")
        if restore_data is None:
            self.generate_positions()
        else:
            self.pos = Position.restore_positions(self.exchange, restore_data, self.watch_list, self.BASE_VALUE,
                                                  market_review=self.market_streamer.reviews,
                                                  base_timeframe=self.BASE_TIMEFRAME)



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
        for market, pos in self.pos.items():
            if pos.slot is not None:

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
        buy_rank, sell_rank, both_rank = self.market_streamer.get_ranked(self.BASE_TIMEFRAME, skip_buys=self.active_buys, skip_sells=self.active_sells, skip_both=self.active_both)
        buy_rank = [m for m in buy_rank.keys() if m not in self.restricted]
        sell_rank = [m for m in sell_rank.keys() if m not in self.restricted]
        both_rank = [m for m in both_rank.keys() if m not in self.restricted]
        while not self.slots.empty():
            slot = self.slots.get()
            if slot == "buy":
                new = buy_rank.pop(0)
                while new in self.active_both and len(buy_rank) > 0:
                    new = buy_rank.pop(0)
                self.open_buy_position(new)

            if slot == "sell":
                new = buy_rank.pop(0)
                while new in self.active_both and len(sell_rank) > 0:
                    new = sell_rank.pop(0)
                self.open_sell_position(new)

            if slot == "both":
                new = both_rank.pop(0)
                self.open_both_position(new)

    def open_buy_position(self, market):
        if market not in self.active_both:
            self.place_orders(market, "buy")
            self.active_buys.add(market)
            self.pos[market].slot = "buy"
            self.active_markets.add(market)

    def open_sell_position(self, market):
        if market not in self.active_both:
            self.active_sells.add(market)
            self.place_orders(market, "sell")
            self.pos[market].slot = "sell"
            self.active_markets.add(market)

    def open_both_position(self, market):
        self.active_both.add(market)
        if market in self.active_buys:
            self.active_buys.discard(market)
        else:
            self.place_orders(market, "buy")
        if market in self.active_sells:
            self.active_sells.discard(market)
        else:
            self.place_orders(market, "sell")
        self.pos[market].slot = "both"
        self.active_markets.add(market)

    def place_orders(self, market: str, side: str):
        self.pos[market].parse_review(self.market_streamer.reviews[market])
        self.pos[market].create_orders_by_side(self.exchange, side, target_factor=self.TARGET_FACTOR, logger=self.logger)
        self.add_to_watcher(self.pos[market])

    def cleaning_routine(self):
        self.log("cleaning routine started")
        self.clean_open_positions()
        self.clean_slots()
        self.clean_open_orders()
        self.get_open_slots()

    def clean_slots(self):
        self.active_markets = []
        for s, pos in self.pos.items():
            if pos.is_open:
                self.active_markets.append(s)
            else:
                self.exchange.cancel_all_orders(symbol=s)

    def clean_open_orders(self):
        open_orders = self.fetch_open_orders()
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

    def clean_open_positions(self, force_fetch=False):
        if time.time() - 60 < self.last_pos_update and not force_fetch:
            return

        for s, pos in self.pos.items():
            pos.is_open = False

        open_pos = self.fetch_open_positions()
        open_syms = [p["symbol"] for p in open_pos]
        self.log(f"clean opos found {len(open_syms)} positions: {open_syms}")
        for p in open_pos:
            self.pos[p["symbol"]].is_open = True
            self.pos[p["symbol"]].update(p)
            self.pos[p["symbol"]].check_risk_status(self.RISK_VALUE)
            if self.pos[p["symbol"]].is_risk and p["symbol"] not in self.risk_positions:
                self.new_risk_positions.add(p["symbol"])
        self.last_pos_update = time.time()

    def reset_closed_positions(self):
        for pos in self.pos.values():
            if not pos.is_open:
                pos.reset()
                self.exchange.cancel_all_orders(symbol=pos.symbol)

    def handle_risk_positions(self):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        for sym in self.new_risk_positions:
            self.exchange.cancel_all_orders()
            self.delegate_risk_position(self.pos[sym])

            # self.pos[sym].place_next_dca_order(self.exchange, logger=self.logger)
            # self.pos[sym].set_closing_order(self.exchange, profit_target=0.003, logger=self.logger)
            self.pos[sym].reset_timeframe()
            self.pos[sym].parse_current_market_states(self.market_streamer.reviews)
            self.place_orders(sym, "both")

        self.new_risk_positions = set()

    def order_closed(self, msg):
        symbol = msg[1]
        exit_order = msg[3]
        res = self.pos[symbol].parse_exit_order(self.exchange, exit_order)
        if res is not None:
            self.log(f"replacing OrderPair for {symbol} at {res}")

    def range_break(self, msg):
        self.clean_open_positions()
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
                    self.active_markets.discard(symbol)
                    self.active_both.discard(symbol)
                    self.active_sells.discard(symbol)
                    self.active_buys.discard(symbol)
                self.pos[symbol].parse_current_market_states(marketReviewer.get_states([symbol]))


    ### UTILITIES

    @decor.safe_request
    def fetch_open_positions(self):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        _pos = self.exchange.fetch_positions()
        return [p for p in _pos if p["contracts"] > 0]

    @decor.safe_request
    def fetch_open_orders(self):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        return self.exchange.fetch_open_orders()

    @decor.safe_request
    def fetch_open_orders_market(self, market):
        self.exchange = mh.change_sub(self.exchange, self.subaccount)
        return self.exchange.fetch_open_orders(market)



    def _parse_review(self, market):
        review = self.market_streamer.reviews[market]
        self.pos[market].parse_current_market_review(review)

    def _init_open_pos_slot(self, market):
        self.active_markets.add(market)
        open_orders = self.fetch_open_orders_market(market)
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
            self.active_both.add(market)
            return "both"
        elif "sell" in ds:
            self.active_sells.add(market)
            return "sell"
        elif "buy" in ds:
            self.active_buys.add(market)
            return "buy"

    def set_dexit(self, pos: Position, cancel_orders=True):
        if cancel_orders:
            self.exchange.cancel_all_orders(symbol=pos.symbol)
        pos.set_closing_order(self.exchange, profit_target=self.DEXIT_TARGET, logger=self.logger)
        pos.place_next_dca_order(self.exchange, logger=self.logger)


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
        market_review = marketReviewer.fetch_states(watch_list, timeframes)
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




