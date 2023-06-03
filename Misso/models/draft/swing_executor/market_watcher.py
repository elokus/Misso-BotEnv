import time
import queue
import threading
import ccxt
import Misso.services.helper as mh

watch_config=dict(schedule={"watch_ranges":{"tx":None, "freq":120},
                            "watch_exits":{"tx": None, "freq":300}},
                  subacc1={"BTC/USD:USD":{"timeframe":"1m", "current_range":[20098, 23489]},
                           "ETH/USD:USD":{"timeframe":"1m", "current_range":[20098, 23489]},
                           "CEL/USD:USD":{"timeframe":"1m", "current_range":[20098, 23489]}})

class OrderWatcher:
    def __init__(self, subaccounts, exchange, active_markets):
        self.subaccounts = subaccounts
        self.exchange = exchange
        self.active_markets = active_markets
        self.watch_schedule = {}
        self.closed_oids = set()

        self.sub_count = {sub:{} for sub in subaccounts}
        self.closed_exits = {sub:{} for sub in subaccounts}
        self.total_exits = {}
        self.total_exits_pct = {}

    def watch_exits(self, name="watch_exits"):
        alerts = []
        for sub in self.subaccounts:
            self.exchange.headers["FTX-SUBACCOUNT"] = sub
            for sym in self.active_markets[sub]:
                self._init_sym_dicts(sym, sub)
                os = self.exchange.fetch_orders(sym, params={"type":"stop"})
                self._track_timestamp(name, sub)
                msg = self.parse_exit_orders(os, sub, sym)
                alerts.extend(msg)
        return alerts

    def parse_exit_orders(self, os: list, sub: str, sym: str, msg="exit_order_closed"):
        out = []
        for o in os:
            self.sub_count[sub][sym]["total"] += 1
            if o["status"] != "closed":
                self.sub_count[sub][sym]["open"] += 1
                continue

            if o["id"] not in self.closed_oids:
                out.append((sub, sym, msg, o))
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
    def __init__(self, watch_config: dict, new_markets: queue.Queue, out_msg: queue.Queue, **kwargs):
        """:param watch_mandate => subacc: *[{active markets: timeframe, range, closed_exit_orders}]  """
        #
        #
        self.subaccounts = ["HFT", "test_strategy", "testing_2", "testing_3", "SwingBot"]
        self.exchange = mh.initialize_exchange_driver("Main")
        self.active_markets = {sub: list(m.keys()) for sub, m in watch_config.items()}
        self.markets = set()
        super().__init__(self.subaccounts, self.exchange, self.active_markets)

        self.new_markets = new_markets
        self.out_msg = out_msg
        self.watch_schedule = None
        self.watch_configs = None
        self._init_configs(watch_config)
        #{"watch_ranges":{"tx":int(time.time()), "freq": 120},
        #                      "watch_exits" :{"tx":int(time.time()), "freq": 300}}

    def run_in_thread(self):
        thread = threading.Thread(target=self.loop_in_thread, args=())
        thread.start()
        while not self.first_update:
            time.sleep(1)

    def loop_in_thread(self):
        self.exchange = mh.initialize_exchange_driver("Main")
        while True:
            self.run_watcher()
            print("restarting create_and_run_update_tasks()")

    def run_watcher(self):
        while not self.new_markets.empty():
            new = self.new_markets.get()
            self.add_market(new)
        r_alerts = self.watch_ranges()
        e_alerts = self.watch_exits()
        for msg in r_alerts:
            self.out_msg.put(msg)
        for msg in e_alerts:
            self.out_msg.put(msg)
        time.sleep(300)

    def add_market(self, new: dict):
        for sub, m_config in new.items():
            for m, config in m_config.items():
                self.markets.add(m)
                if m not in self.active_markets[sub]:
                    self.active_markets[sub].append(m)
                if m not in self.watch_configs:
                    self.watch_configs[m] = {}
                self.watch_configs[m][sub] = config

    def watch_ranges(self, name="watch_ranges"):
        alerts = []
        tickers = self.exchange.fetch_tickers(self.markets)
        for m, sub_configs in self.watch_schedule.items():
            alerts.extend(MarketWatcher.check_ranges(tickers, m, sub_configs))
        return alerts

    @staticmethod
    def check_ranges(ticker: dict, market: str, sub_configs: dict, msg: str="out_of_range"):
        out = []
        price = ticker[market]["last"] if market in ticker else ticker["last"]
        for sub, config in sub_configs.items():
            if MarketWatcher.price_in_range(price, config["current_range"]):
                continue
            out.append((sub, market, msg, config["timeframe"]))
        return out

    @staticmethod
    def price_in_range(price: float, range: list, buffer: float=0):
        delta = abs(range[0] - range[1]) * buffer
        return price > range[0]-delta and price < range[1]+delta

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

class osubHandler:
    def __init__(self, subaccounts, exchange, active_markets):
        self.subaccounts = subaccounts
        self.exchange = exchange
        self.active_markets = active_markets

        #open positions => opos
        #open orders => oord
        #open cond orders => ocord
        self.opos = {}
        self.oord = {}
        self.ocord = {}



    def get_opos_by_sub(self, subs: list=None):
        subs = subs or self.subaccounts
        for sub in subs:
            self.opos[sub] = osubHandler.get_opos(self.exchange, sub)

    def close_opos(self, opos:dict=None):
        opos = opos or self.opos
        pending = []
        for sub, opos in opos.items():
            _opos = [p for p in opos if p["unrealizedPnl"] > 0]
            osubHandler.set_closing_orders(self.exchange, sub, _opos)
            pending.extend([(sub, p["symbol"]) for p in _opos])
        return pending


#raise NetworkError(details) from e

    @staticmethod
    def get_oord(exchange: ccxt, sub: str, params: dict=None):
        exchange = mh.change_sub(exchange, sub)
        return exchange.fetch_open_orders(params=params)

    @staticmethod
    def get_coord(exchange: ccxt, sub: str):
        exchange = mh.change_sub(exchange, sub)
        return exchange.fetch_open_orders(params={"type":"stop"})

    @staticmethod
    def get_opos(exchange: ccxt, sub: str):
        exchange = mh.change_sub(exchange, sub)
        pos = exchange.fetch_positions()
        return [p for p in pos if p["contracts"] > 0]

    @staticmethod
    def close_opos_subs(exchange: ccxt, subs: list, profit_target: float=0.005, cancel_all: bool=False, in_profit:bool=True):
        orders = {}
        for sub in subs:
            orders[sub] = osubHandler.set_closing_orders(exchange, sub, profit_target, cancel_all, in_profit)
        return orders


    @staticmethod
    def set_closing_orders(exchange: ccxt, sub: str, profit_target: float=0.005, cancel_all: bool=False, in_profit:bool=True, opos: list=None):
        import time
        ftx = mh.change_sub(exchange, sub)
        if cancel_all:
            ftx.cancel_all_orders()
        pos = opos or ftx.fetch_positions()
        orders = []
        for p in pos:
            if p["contracts"] > 0:
                if in_profit:
                    orders.append(mh.get_exit_order_in_profit(p, profit_target, ftx))
                else:
                    orders.append(mh.get_exit_order_at_last(p, ftx))
        for o in orders:
            try:
                mh.create_limit_order(ftx, o)
                time.sleep(0.2)
            except Exception as err:
                print(f"ERROR occured with {o[4]}:")
                print(err)
        return orders

    @staticmethod
    def dca_all_positions(exchange: ccxt, sub: str, opos: list=None, min_range: float=0.005, cancel_all=True):
        ftx = mh.change_sub(exchange, sub)
        if cancel_all:
            ftx.cancel_all_orders()
        pos = opos or ftx.fetch_positions()
        for p in pos:
            if p["contracts"] > 0:
                order = mh.get_dca_order(ftx, p, min_range)
                try:
                    mh.create_limit_order(ftx, order)
                except Exception as err:
                    print(f"ERROR occured with {order[4]}:")
                print(err)

    @staticmethod
    def get_active_markets(exchange: ccxt, sub: str):
        exchange = mh.change_sub(exchange, sub)

        pos = exchange.fetch_positions()
        orders = exchange.fetch_open_orders()
        exits = exchange.fetch_open_orders(params={"type":"stop"})

        op = [p["symbol"] for p in pos if p["contracts"] > 0]
        _oo = [o["symbol"] for o in orders]
        _oe = [o["symbol"] for o in exits]
        opo = [o for o in orders if o["symbol"] in op]
        ope = [o for o in opo if o["clientOrderId"] is None]
        ope = [o for o in [*ope, *exits] if o["symbol"] in op]
        return dict(orders_only=[o for o in _oo if not o in op], active=op, active_orders=opo, active_exits=ope)




