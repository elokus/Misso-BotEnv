import ccxt, queue
import Misso.services.helper as mh
from Misso.models.streamer.market_streamer import MarketStreamer
from Misso.models.objects import Position
from Misso.models.draft.messaging import MessageHandler

class SubManager(MessageHandler):
    def __init__(self, subaccount: str, **kwargs):
        self.name = str(__class__.__name__) + "_" + subaccount
        self.subaccount = subaccount

        self._exchange: ccxt = None
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


        self.pos = {}
        self.active_buys = set()
        self.active_sells = set()
        self.active_both = set()
        self.slots = queue.Queue()

        self.active_watcher = {}
        self.init_sub_client()



    def init_sub_client(self):
        self.init_market_streamer()
        self.init_positions()
        self.get_open_slots()

    def run_client(self):
        self.handle_in_msgs()
        self.handle_slots()
        self.handle_active()
        self.handle_out_msg()

    def handle_in_msgs(self):
        self.message_processing()

    def handle_slots(self):
        pass

    def handle_active(self):
        pass

    def handle_out_msgs(self):
        pass
        self.run_strategy()


    @property
    def exchange(self):
        return mh.change_sub(self._exchange, self.subaccount)

    @property
    def open_pos(self):
        return {market:pos for market, pos in self.pos.items() if pos.is_open}

    @property
    def closed_pos(self):
        return {market:pos for market, pos in self.pos.items() if not pos.is_open}

    @property
    def blocked_slots(self):
        opos = self.open_pos.values()
        slots = [pos for pos in opos if pos.slot is not None]
        return {x:slots.count(x) for x in slots}






    def init_positions(self):
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