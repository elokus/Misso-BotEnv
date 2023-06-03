import time, queue, ccxt
import Misso.services.cust_decorator as deco
import Misso.services.helper as mh

watch_config=dict(schedule={"watch_ranges":{"tx":None, "freq":120},
                            "watch_exits":{"tx": None, "freq":300}},
                  subacc1={"BTC/USD:USD":{"timeframe":"1m", "current_range":[20098, 23489]},
                           "ETH/USD:USD":{"timeframe":"1m", "current_range":[20098, 23489]},
                           "CEL/USD:USD":{"timeframe":"1m", "current_range":[20098, 23489]}})

class OrderWatcher:
    def __init__(self, subaccounts, exchange, active_markets, logger=None):
        self.subaccounts = subaccounts
        self.exchange = exchange
        self.active_markets = active_markets
        self.watch_schedule = {}
        self.closed_oids = set()
        self.since = int(time.time()) * 1000
        self.logger = logger

        self.sub_count = {sub:{} for sub in subaccounts}
        self.closed_exits = {sub:{} for sub in subaccounts}
        self.total_exits = {}
        self.total_exits_pct = {}

    def watch_exits(self, name="watch_exits"):
        alerts = []
        timestamp = int(time.time()-60) * 1000
        for sub in self.subaccounts:
            self.exchange.headers["FTX-SUBACCOUNT"] = sub
            os = self.exchange.fetch_orders(since=self.since, params={"type":"stop"})
            if len(os) > 0:
                for o in os:
                    if o["symbol"] in self.active_markets[sub]:
                        sym = o["symbol"]
                        self._init_sym_dicts(sym, sub)
                        self._track_timestamp(name, sub)
                        msg = self.parse_exit_order(o, sub, sym)
                        alerts.extend(msg)
        self.since = timestamp
        return alerts

    def parse_exit_order(self, o: dict, sub: str, sym: str, msg="exit_order_closed"):
        out = []
        if o["status"] == "closed":
            if o["id"] not in self.closed_oids:
                out.append(("sub_manager",[sub, sym, msg, o]))
                self.closed_oids.add(o["id"])
                self.closed_exits[sub][sym].append(o)
                self.sub_count[sub][sym]["closed"] += 1
        return out

    def sub_order_stats(self):
        for sub in self.subaccounts:
            _c, _o, _t = 0, 0, 0
            for sym in self.active_markets[sub]:
                _c += self.sub_count[sub][sym]["closed"]
                _o += self.sub_count[sub][sym]["open"]
                _t = _c + _o
            self.total_exits[sub] = dict(closed=_c, open=_o, total=_t)
            self.total_exits_pct[sub] = dict(closed=round(_c/_t, 2), open=round(_o/_t,2))

    def _init_sym_dicts(self, sym, sub):
        if sym not in self.sub_count[sub]:
            self.sub_count[sub][sym] = {"closed":0, "open": 0, "total": 0}
        if sym not in self.closed_exits[sub]:
            self.closed_exits[sub][sym] = []

    def _track_timestamp(self, proc_name:str, sub: str):
        if proc_name not in self.watch_schedule:
            self.watch_schedule[proc_name] = {sub: int(time.time())}
        else:
            self.watch_schedule[proc_name][sub] = int(time.time())


class MarketWatcher(OrderWatcher):
    def __init__(self, in_msg: queue.Queue, out_msg: queue.Queue, subaccounts=["HFT", "test_strategy", "testing_2", "testing_3", "SwingBot"], logger=None, **kwargs):
        """:param watch_mandate => subacc: *[{active markets: timeframe, range, closed_exit_orders}]  """
        #
        #
        self.subaccounts = subaccounts
        self.exchange = mh.initialize_exchange_driver("Main")
        self.active_markets = {sub: [] for sub in self.subaccounts}
        self.markets = set()
        self.logger = logger
        super().__init__(self.subaccounts, self.exchange, self.active_markets, logger=self.logger)

        self.in_msg = in_msg
        self.out_msg = out_msg

        self.watch_configs = {}


    @deco.run_in_thread_deamon
    def run(self):
        self.exchange = mh.initialize_exchange_driver("Main")
        while True:
            self.run_watcher()


    def run_watcher(self):
        while not self.in_msg.empty():
            new = self.in_msg.get()
            self.handle_message(new)
        r_alerts = self.watch_ranges()
        e_alerts = self.watch_exits()
        for msg in r_alerts:
            self.out_msg.put(msg)
        for msg in e_alerts:
            self.out_msg.put(msg)
        time.sleep(60)

    def handle_message(self, new: dict):
        self.log(f"WATCHER ADDING new market {new}")
        for sub, m_config in new.items():
            for m, config in m_config.items():
                if config == "remove":
                    self.remove_market(m, sub)
                else:
                    self.add_market(config, m, sub)

    def remove_market(self, m, sub):
        if m in self.active_markets[sub]:
            self.active_markets[sub].remove(m)
        if sub in self.watch_configs[m]:
            self.watch_configs[m].pop(sub)

    def add_market(self, config, m, sub):
        self.markets.add(m)
        if sub not in self.active_markets:
            self.active_markets[sub] = {}
        if m not in self.active_markets[sub]:
            self.active_markets[sub].append(m)
        if m not in self.watch_configs:
            self.watch_configs[m] = {}
        self.watch_configs[m][sub] = config

    def watch_ranges(self, name="watch_ranges"):
        alerts = []
        tickers = self.exchange.fetch_tickers(self.markets)
        for m, sub_configs in self.watch_configs.items():
            alerts.extend(MarketWatcher.check_ranges(tickers, m, sub_configs))
        return alerts

    @staticmethod
    def check_ranges(ticker: dict, market: str, sub_configs: dict, msg: str="out_of_range"):
        out = []
        price = ticker[market]["last"] if market in ticker else ticker["last"]
        for sub, config in sub_configs.items():
            if MarketWatcher.price_in_range(price, config["mr"]):
                continue
            payload = {"tx": config["tx"], "side": "buy" if price <= config["mr"][0] else "sell"}
            out.append(("sub_manager",[sub, market, msg, payload]))
        return out

    @staticmethod
    def price_in_range(price: float, range: list, buffer: float=0):
        delta = abs(range[0] - range[1]) * buffer
        return price >= range[0]-delta and price <= range[1]+delta

    def _init_configs(self, watch_mandate: dict):
        config_map = {}
        for sub, markets in watch_mandate.items():
            if sub == "schedule":
                self.watch_schedule = markets
                continue

            self.markets.update(list(markets.keys()))
            for m, config in markets.items():
                if m not in config_map:
                    config_map[m] = {sub:config}
                else:
                    config_map[m][sub] = config
        self.watch_configs = config_map  #market/sub/instruction

    def log(self, msg):
        if self.logger is None:
            print(f"[WATCHER]: {msg}")
        else:
            self.logger.info(f"[WATCHER]: {msg}")


