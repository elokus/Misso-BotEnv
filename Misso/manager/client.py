import queue, ccxt, time, threading

from Misso.manager.message_client import MessageClient
from Misso.models.position import position_factory
import Misso.services.utilities as utils
import Misso.services.cust_decorator as decor
import threading



class SubClient(MessageClient):
    def __init__(self, subaccount, **kwargs):
        super().__init__()
        self.sub = subaccount
        self.name = str(__class__.__name__) + "_" + subaccount
        self.logger = self.init_default("logger", kwargs)
        self.in_msg = self.init_default("in_msg", kwargs)
        self.out_msg = self.init_default("out_msg", kwargs)
        self.client_config = self.init_default("client_config", kwargs)
        self.run_id = self.init_default("run_id", kwargs)
        self.semaphore: threading.Semaphore = self.init_default("semaphore", kwargs)

        self.TIMEOUT = 0
        self.CLEAN_AFTER = 1200
        self._should_clean = 0

        self.watch_list = None
        self.restricted = None
        self.market_streamer = None
        self.store = None
        self.pos = {}
        self.n_run = 0

        self._exchange: ccxt = None
        self.parameter = self.init_default("parameter", kwargs)
        self.init_kwargs(**kwargs)

    def init_kwargs(self, **kwargs):
        if len(kwargs) > 0:
            for attr, arg in kwargs.items():
                setattr(self, attr, arg)
        if hasattr(self, "config"):
            for key, value in self.config.items():
                setattr(self, key, value)


    def init_client(self):
        self.init_markets()
        self.restore_client()
        self.init_positions()
        self.init_strategy()

    @decor.run_in_thread_deamon
    def start(self):
        self._start()
        while True:
            if self.should_clean:
                self.cleaning_routine()
            if not self.in_msg.empty() or not self.slots.empty():
                self.run_client()
            time.sleep(2)


    @decor.with_semaphore
    def run_client(self):
        self.before_run()
        self.log("before run finished")
        self.handle_in_msgs()
        self.log("handle in msg finished")
        self.handle_slots()
        self.log("handle slots finished")
        self.handle_active()
        self.log("handle active finished")
        self.handle_out_msgs()
        self.log("handle out msgs finished")
        self.after_run()
        self.n_run += 1

    def init_markets(self):
        self.watch_list = utils.get_watch_list(from_file=True)

    def init_positions(self):
        for market in self.watch_list:
            self.init_position(market)

    def init_position(self, market):
        pos_base = self.POSITION_BASE if hasattr(self, "POSITION_BASE") else None
        if market not in self.pos:
            self.pos[market] = position_factory(pos_base)(market, **self._position_params(market))

    def restore_client(self):
        if hasattr(self, "RESTORE"):
            if self.RESTORE:
                self._restore_client()

    def init_strategy(self):
        pass

    def handle_slots(self):
        pass

    def handle_active(self):
        pass

    def handle_out_msgs(self):
        pass

    def init_default(self, attr, kwargs):
        if attr in kwargs:
            return kwargs.pop(attr)
        if attr == "in_msg" or attr == "out_msg":
            return queue.Queue()
        return None

    def log(self, msg):
        self.logger.info(f"{self.name}: {msg}")

    def update_pos(self, pos):
        resp = self.fetch_position(pos.symbol)
        if resp is not None:
            pos.update_live(resp)
        return pos

    @property
    def exchange(self):
        return utils.change_sub(self._exchange, self.sub)

    @property
    def open_pos(self):
        return [pos for market, pos in self.pos.items() if pos.is_open]

    @property
    def closed_pos(self):
        return [pos for market, pos in self.pos.items() if not pos.is_open]

    @property
    def active_pos(self):
        return [pos for market, pos in self.pos.items() if pos.is_active]

    @property
    def blocked_slots(self):
        opos = self.open_pos
        slots = [pos.slot for pos in opos if pos.slot is not None]
        return {x:slots.count(x) for x in slots}

    @property
    def blocked_active_slots(self):
        apos = self.active_pos.copy()
        slots = [pos.slot for pos in apos if pos.slot is not None]
        return {x:slots.count(x) for x in slots}

    @decor.safe_request
    def fetch_open_positions(self):
        _pos = self.exchange.fetch_positions()
        return [p for p in _pos if p["contracts"] > 0]

    @decor.safe_request
    def fetch_position(self, market: str):
        pos = self.exchange.fetch_positions([market])
        if len(pos) > 0:
            return pos[0]
        return None

    @decor.safe_request
    def fetch_open_orders(self):
        return self.exchange.fetch_open_orders()

    @decor.safe_request
    def fetch_open_orders_market(self, market):
        return self.exchange.fetch_open_orders(market)

    @property
    def should_clean(self):
        if self.CLEAN_AFTER > 0:
            return self._should_clean + self.CLEAN_AFTER <= time.time()
        return False

    def save_positions(self):
        self.store.store_positions(self.sub, self.pos, self.run_id)
        self.log("saved positions to file")

    def set_next_clean(self):
        self._should_clean = time.time()

    def _start(self):
        if self.TIMEOUT > 0:
            self.log(f"starting in {self.TIMEOUT} seconds")
            time.sleep(self.TIMEOUT)
        self.log("client started.")

    def _position_params(self, market):
        pass

    def _restore_client(self):
        pass

    def before_run(self):
        pass

    def after_run(self):
        pass
