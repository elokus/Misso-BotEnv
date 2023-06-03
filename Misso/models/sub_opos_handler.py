import Misso.services.helper as mh
import time, queue, ccxt

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
            _opos = [p for p in opos if p["contracts"] > 0]
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
    def close_opos_subs(exchange: ccxt, subs: list, profit_target: float=0.005, cancel_all: bool=False, in_profit:bool=True, max_value=40):
        orders = {}
        for sub in subs:
            orders[sub] = osubHandler.set_closing_orders(exchange, sub, profit_target, cancel_all, in_profit, max_value=max_value)
        return orders


    @staticmethod
    def set_closing_orders(exchange: ccxt, sub: str, profit_target: float=0.005, cancel_all: bool=False, in_profit:bool=True, opos: list=None, max_value=0):
        import time
        ftx = mh.change_sub(exchange, sub)
        if cancel_all:
            ftx.cancel_all_orders()
        pos = opos or ftx.fetch_positions()
        orders = []
        for p in pos:
            if p["contracts"] > 0:
                if max_value > 0 and p["notional"] > max_value:
                    continue
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



