import time, queue, ccxt, logging
import Misso.services.cust_decorator as deco
import Misso.services.utilities as utils
import Misso.services.helper as mh
from Misso.models.store import Store



class OrderWatcher:
    def __init__(self):
        super().__init__()
        self.exchange = None
        self.active_sub_markets = None
        self.logger = None
        self.init_orders = False

        self.closed_orders = set()
        self.watch_ids = set()
        self.matched_ids = {}
        self.matched_orders = {}
        self.closed_ids = set()
        self.closed_sids = set()

        self.ow_jobs = {}
        self.ow_out_msg = queue.Queue()
        self.tx_info = {}
        self.last_store_tx = 0
        self.log("OrderWatcher initialized")


    def init_from_store(self):
        stored = self.store.load_orders(default_subs=self.subaccounts)
        self.closed_orders = set(stored["closed_orders"])
        self.matched_orders = stored["matched_orders"]
        self.tx_info = stored["info"]
        self.init_fetch_orders()

    def init_fetch_orders(self):
        now = self.now_unix_s
        for sub in self.subaccounts:
            self.fetch_historic_orders(self.tx_info[sub], now, sub)
            self.fetch_historic_stops(self.tx_info[sub], now, sub)
            self.tx_info[sub] = now*1000
        self.get_closed_match_stops()
        self.store_orders()

    def fetch_historic_orders(self, since, till, sub):
        self.exchange = utils.change_sub(self.exchange, sub)
        while self._safe_time_ms(till) >= self._safe_time_ms(since):
            orders = self.safe_fetch_orders_before(till)
            till = orders[0]["timestamp"]
            self.closed_orders.update([o["id"] for o in orders if o["status"] == "closed"])
            if len(orders) < 200:
                break

    def fetch_historic_stops(self, since, till, sub):
        self.exchange = utils.change_sub(self.exchange, sub)
        while self._safe_time_ms(till) >= self._safe_time_ms(since):
            orders = self.safe_fetch_stops_before(till)
            csos, ccsos = self._match_stop_orders(orders)
            till = orders[0]["timestamp"]
            self.closed_orders.update(ccsos)
            self.closed_sids.update(csos)
            if len(orders) < 200:
                break

    def store_orders(self):
        if self.last_store_tx + 600 <= self.now_unix_s:
            self.store.save_orders(self.tx_info, self.closed_orders, self.matched_orders)
            self.log("closed_orders stored")
            self.last_store_tx = self.now_unix_s


    def add_order_id(self, sub: str, payload: tuple):
        oid, market = payload
        self.watch_ids.add(oid)
        self.ow_jobs[oid] = (sub, market)

    def add_init_order_id(self, sub: str,payload: tuple):
        self.init_orders = True
        self.add_order_id(sub, payload)

    def watch_orders(self):
        self.init_fetch_orders()
        for oid in self.watch_ids.copy():
            if oid in self.closed_orders:
                self.watch_ids.discard(oid)
                self.ow_out_msg.put(("sub_client",(self.ow_jobs[oid][0], "order_closed", self.ow_jobs[oid][1], oid)))
                del self.ow_jobs[oid]


    def update_closed_orders(self, sub):
        self.closed_orders.update(self.fetch_closed_orders(sub))
        csos, ccsos = self.fetch_match_closed_stops(sub)
        self.closed_sids.update(csos)
        self.closed_orders.update(ccsos)
        self.get_closed_match_stops()

    def get_closed_match_stops(self):
        for soid in self.closed_sids.copy():
            if soid in self.matched_orders:
                if self.matched_orders[soid] in self.closed_orders:
                    self.closed_orders.add(soid)
                    self.closed_sids.discard(soid)

    def fetch_closed_orders(self, sub):
        self.exchange = utils.change_sub(self.exchange, sub)
        orders = self.safe_fetch_orders(self.last_since)
        closed = [o["id"] for o in orders if (o["status"] == "closed" or o["status"] == "cancelled" or o["status"] == "canceled")]
        return closed

    def fetch_match_closed_stops(self, sub):
        self.exchange = utils.change_sub(self.exchange, sub)
        sorders = self.safe_fetch_stop_orders(self.last_since)
        csos, ccsos = self._match_stop_orders(sorders)
        return csos, ccsos

    def _match_stop_orders(self, sorders):
        csos = {o["id"]: {"symbol": o["symbol"], "price": float(o["info"]["orderPrice"]), "amount": o["amount"],
                          "side": o["side"], "timestamp": o['lastTradeTimestamp']} for o in sorders if
                o["status"] == "closed"}
        for soid, cso in csos.items():
            if soid not in self.matched_orders and soid not in self.closed_orders:
                oid = self._match_order_id(cso)
                if oid is not None:
                    self.matched_orders[soid] = oid
        return list(csos.keys()), [o["id"] for o in sorders if o["status"] == "cancelled" or o["status"] == "canceled"]

    def _match_order_id(self, cso: dict):
        orders = self.safe_fetch_match_order(cso)
        for o in orders:
            if o["timestamp"] - 10000 < cso["timestamp"] and o["price"] == cso["price"] and o["side"] == cso["side"]:
                return o["id"]
        return None

    @property
    def now_unix_s(self):
        return time.time()

    @property
    def now_unix_ms(self):
        return time.time() * 1000

    def _safe_time_ms(self, tx: float):
        if tx < 10**12:
            tx = tx * 1000
        return float(int(tx))

    def _safe_time_s(self, tx: float):
        if tx > 10**12:
            tx = tx/1000
        return float(int(tx))

    @deco.safe_request
    def safe_fetch_stop_orders(self, since):
        return self.exchange.fetch_orders(since=since, params={"type":"stop"})

    @deco.safe_request
    def safe_fetch_match_order(self, cso):
        till = self._safe_time_s(cso["timestamp"])+60
        res = self.exchange.fetch_orders(cso["symbol"], limit=100, params={"end_time":till})
        res.reverse()
        return res

    @deco.safe_request
    def safe_fetch_orders(self, since):
        return self.exchange.fetch_orders(since=since)

    @deco.safe_request
    def safe_fetch_orders_before(self, till):
        till = self._safe_time_s(till)
        return self.exchange.fetch_orders(limit=200, params={"end_time": till})

    @deco.safe_request
    def safe_fetch_stops_before(self, till):
        till = self._safe_time_s(till)
        return self.exchange.fetch_orders(limit=200, params={"end_time": till, "type":"stop"})



class MarketWatcher:
    def __init__(self):
        super().__init__()
        self.exchange = None
        self.logger = None
        self.active_sub_markets = None
        self._remove_markets = []

        self.mw_out_msg = queue.Queue()
        self.mw_jobs = {}
        self.log("MarketWatcher initialized")

    def add_market(self, sub: str, payload: tuple):
        market, job = payload
        self.active_sub_markets[sub].add(market)
        self.add_job(market, sub, job)

    def add_job(self, market: str, sub: str, job:dict):
        if market not in self.mw_jobs:
            self.mw_jobs[market] = {}
        self.mw_jobs[market][sub] = job

    def remove_markets(self):
        while len(self._remove_markets) > 0:
            sub_market = self._remove_markets.pop()
            self.remove_market(*sub_market)

    def remove_market(self, sub: str, market: str):
        self.active_sub_markets[sub].discard(market)
        self.remove_job(market, sub)

    def remove_job(self, market: str, sub: str):
        if sub in self.mw_jobs[market]:
            self.mw_jobs[market].pop(sub)

    def watch_breakouts(self):
        tickers = self.exchange.fetch_tickers()
        mw_jobs = self.mw_jobs.copy()
        for market, _jobs in mw_jobs.items():
            if market in tickers:
                price = tickers[market]["last"]
                jobs = _jobs.copy()
                for sub, job in jobs.items():
                    if price < job["range"][0]:
                        self.process_low_breakout(sub, market, job)
                    elif price > job["range"][1]:
                        self.process_high_breakout(sub, market, job)
        self.remove_markets()

    def process_low_breakout(self, sub: str, market: str, job: dict):
        self.mw_out_msg.put(("sub_client", (sub, "low_breakout", market, job)))
        self._remove_markets.append((sub, market))

    def process_high_breakout(self, sub: str, market: str, job: dict):
        self.mw_out_msg.put(("sub_client", (sub, "high_breakout", market, job)))
        self._remove_markets.append((sub, market))






class Watcher(MarketWatcher, OrderWatcher):
    def __init__(self, in_msg: queue.Queue, out_msg: queue.Queue, subaccounts: list, logger: logging.Logger, store: Store=None):
        super().__init__()
        self.subaccounts = subaccounts
        self.in_msg = in_msg
        self.out_msg = out_msg
        self.exchange = mh.initialize_exchange_driver("Main")
        self.logger = logger
        self.active_sub_markets = {sub: set() for sub in self.subaccounts}
        self.store = store if store is not None else Store()
        self.init_from_store()
        self.log("Watcher initialized")

    @deco.run_in_thread_deamon
    def run(self):
        self.exchange = mh.initialize_exchange_driver("Main")
        try:
            while True:
                self.message_handler()
                try:
                    self.run_market_watcher()
                except Exception as e:
                    self.log("ERROR run market_watcher BEAKOUTS")
                    self.logger.error(e, exc_info=True)
                try:
                    self.run_order_watcher()
                except Exception as e:
                    self.log("ERROR run order WATCHER Order")
                    self.logger.error(e, exc_info=True)
                time.sleep(20)
        finally:
            self.save()


    def message_handler(self):
        while not self.in_msg.empty():
            job = self.in_msg.get()
            self.prepare_job(job)

    def run_market_watcher(self):
        self.watch_breakouts()
        while not self.mw_out_msg.empty():
            msg = self.mw_out_msg.get()
            self.out_msg.put(msg)

    def run_order_watcher(self):
        self.watch_orders()
        while not self.ow_out_msg.empty():
            msg = self.ow_out_msg.get()
            self.out_msg.put(msg)

    def prepare_job(self, job: dict):
        subject, sub, payload = job
        self.log(f"prepare job: {job}")

        if subject == "add_mw":
            self.add_market(sub, payload)

        if subject == "remove_mw":
            self.remove_market(sub, payload)

        if subject == "add_order_id":
            self.add_order_id(sub, payload)

        if subject == "add_client_id":
            pass
            #self.add_client_id(sub, payload)

        if subject == "add_init_order_id":
            self.add_init_order_id(sub, payload)


    def log(self, msg):
        if self.logger is None:
            print(f"[WATCHER]: {msg}")
        else:
            self.logger.info(f"[WATCHER]: {msg}")