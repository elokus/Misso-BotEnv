import pandas as pd
import threading
import Misso.services.helper as ph
from Misso.services.draft.swing_levels import a_get_swing_levels
import asyncio
import ccxt.async_support as accxt
import time
import numpy as np


### Verified Functions:
async def gather_swing_levels(exchange, watch_list, timeframe="1m", limit=10000, derivate_num=2):
    tasks = [a_get_swing_levels(exchange, symbol, timeframe, limit=limit, derivate_num=derivate_num) for symbol in watch_list]
    swing_levels = await asyncio.gather(*tasks, return_exceptions=True)
    res = {}
    for swings in swing_levels:
        if isinstance(swings, dict):
            res[swings["symbol"]] = swings
    return res #dict(symbol=symbol, buy_levels=lvl_vol_low, sell_levels=lvl_vol_high, info=info, last=last)


async def get_open_positions(exchange):
    positions = {}
    _positions = await exchange.fetch_positions()
    for pos in _positions:
        if pos["contracts"] > 0:
            positions[pos["symbol"]] = pos
    return positions

async def get_ohlcv_data(exchange, watch_list, timeframe, limit, since=None, close=False):
    if isinstance(watch_list, str):
        watch_list = [watch_list]
    tasks = [_get_ohlcv_data(exchange, symbol, timeframe, limit, since=since) for symbol in watch_list]
    data = await asyncio.gather(*tasks, return_exceptions=True)
    candles = {}
    for d in data:
        for k, v in d.items():
            candles[k] = v
    if close:
        await exchange.close()
    return candles

async def _get_ohlcv_data(exchange: object, symbol: str, timeframe: str, limit: int, since: int=None):
    resp = await exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit, since=since)
    return {symbol:np.array(resp)}

async def get_ohlcv_data_multi_timeframe(exchange, watch_list, limit=500, close=False, since=None, timeframes=["1m", "15m", "1h", "4h"]):
    if exchange is None:
        exchange = accxt.ftx()
    if isinstance(watch_list, str):
        watch_list = [watch_list]
    symbol_timeframes = [(s, t) for s in watch_list for t in timeframes]
    if since is not None:
        since = ph.safe_format_since(since)
    tasks = [_get_ohlcv_data_multi_timeframe(exchange, sym, tx, limit=limit, since=since) for sym, tx in symbol_timeframes]
    data = await asyncio.gather(*tasks, return_exceptions=True)

    candles = {}
    for d in data:
        for k, v in d.items():
            if not k in candles:
                candles[k] = {}
            candles[k][v["timeframe"]] = v["data"]
    if close:
        await exchange.close()
    return candles

async def _get_ohlcv_data_multi_timeframe(exchange: object, symbol: str, timeframe: list, limit: int=500, since: int=None):
    resp = await exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit, since=since)
    return {symbol:{"timeframe":timeframe, "data":np.array(resp)}}

async def get_last_prices_from_watch_list(exchange, watch_list):
    tickers = await exchange.fetch_tickers(watch_list)
    last_price = {}
    for symbol, ticker in tickers.items():
        last_price[symbol] = ticker["last"]
    return last_price

async def get_total_balance(exchange):
    balance = await exchange.fetch_balance()
    return balance["total"]["USD"]

async def get_free_balance(exchange):
    balance = await exchange.fetch_balance()
    return balance["free"]["USD"]

async def get_filtered_watch_list(filter_config={"info.volumeUsd24h": {"value": 2000000, "type": "greater_or_equal", "value_type": "float"}}, restricted=["USDT/USD:USD"]):
    exchange = accxt.ftx()
    markets = await exchange.load_markets()
    futures = ph.get_future_symbol_list_from_markets_dict(markets)
    watch_list = []
    for future in futures:
        if not future in restricted:
            if ph.filter_dict_by_config(markets[future], filter_config):
                watch_list.append(future)
    await exchange.close()
    return watch_list

async def create_limit_order(exchange, order):
    from ccxt.base.errors import InsufficientFunds
    try:
        resp = await exchange.create_limit_order(order[4], order[3], order[1], order[2])
        return [resp["id"], order[1], order[2], order[3], order[4]]
    except InsufficientFunds as err:
        return ["failed", order[1], order[2], "InsufficientFunds", order[4]]
    except Exception as e:
        print(e)
    return

async def get_markets_by_min_spread(ftx: object, min_spread: float=0.001, leave_out: list=["USDT/USD:USD"], min_volume: float= 2000, spread_type: str= "trades", return_spread=False):
    symbol_list = await get_future_symbol_list(ftx, leave_out)
    res = await asyncio.gather(*[fetch_trades_spread(ftx, symbol) for symbol in symbol_list])
    symbols = []
    sym_spread = []
    for sp in res:
        for key, value in sp.items():
            if value["volume"] >= min_volume and value[spread_type] >= min_spread:
                symbols.append(key)
                if return_spread:
                    sym_spread.append((key, value[spread_type]))
    if return_spread:
        return sym_spread
    return symbols


#
##
#### end  ####

#### unverified functions  ###
##
#

async def get_current_ranges(exchange, watch_list):
    tickers = await exchange.fetch_tickers(watch_list)
    tasks = [get_current_range(exchange, symbol, value["last"]) for symbol, value in tickers.items()]
    ranges = await asyncio.gather(*tasks, return_exceptions=True)
    rd = {}
    for r in ranges:
        if isinstance(r, dict):
            for k, v in r.items():
                rd[k] = v
    return rd

async def get_future_symbol_list(exchange: object, leave_outs:list = ["USDT/USD:USD"]):
    markets = await exchange.fetch_markets()
    symbol_list = []
    for m in markets:
        if m["symbol"].endswith("USD:USD") and m["symbol"] not in leave_outs:
            symbol_list.append(m["symbol"])
    return symbol_list

async def get_market_volatility(ftx, min_volume=5000, sort_by="candles_spread"):
    symbol_list = await get_future_symbol_list(ftx)
    res = await asyncio.gather(*[fetch_trades_spread(ftx, symbol) for symbol in symbol_list])
    trades_spread = []
    candles_spread = []
    volumes = []
    symbols = []
    for sp in res:
        for key, value in sp.items():
            if value["volume"] > min_volume:
                symbols.append(key)
                trades_spread.append(value["trades"])
                candles_spread.append(value["candles"])
                volumes.append(value["volume"])
    df = pd.DataFrame({"symbols":symbols, "trades_spread":trades_spread, "candles_spread":candles_spread, "volume":volumes})
    return df.sort_values(by=sort_by, ascending=False)

async def get_volatile_markets(min_volume):
    import ccxt.async_support as ccxt
    exchange = accxt.ftx()
    exchange.rateLimit = 200
    df = await get_market_volatility(exchange, min_volume=min_volume, sort_by="trades_spread")
    await exchange.close()
    symbols = df["symbols"].head(10).values
    print(symbols)
    return ph.filter_symbols_low_volume(symbols)

async def fetch_trades_spread(exchange, symbol):
    try:
        trades = await exchange.fetchTrades(symbol)
    except:
        trades = await exchange.fetchTrades(symbol)
    try:
        candles = await exchange.fetchOHLCV(symbol, timeframe="1m", limit=10)
    except:
        candles = await exchange.fetchOHLCV(symbol, timeframe="1m", limit=10)
    trade_prices = []
    for trade in trades:
        trade_prices.append(trade["price"])
    trades_spread = (max(trade_prices)-min(trade_prices))/min(trade_prices)
    return {symbol:{"trades":trades_spread,
                    "candles":ph.spread_from_candles(candles),
                    "volume":ph.avg_volume_from_candles(candles),}}

async def get_candle_eval_dict(exchange, symbol, key=None, timeframe="1m", limit=5):
    unit_dict = {}
    try:
        _candles = await exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit)
    except:
        await asyncio.sleep(1)
        _candles = await exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit)
    candles = np.array(_candles)
    unit_dict["candles"] = candles
    unit_dict["avg_spread"] = ph.spread_from_candles(candles)
    unit_dict["avg_spread_abs"] = ph.spread_from_candles_abs(candles)
    unit_dict["range"] = ph.range_from_candles(candles)
    unit_dict["range_spread_ratio"] = ph.range_spread_ratio(unit_dict["range"], unit_dict["avg_spread_abs"])
    if key is not None:
        return unit_dict[key]
    return unit_dict

async def fetch_trades_eval(exchange, symbol, return_last=False):
    trades = await exchange.fetchTrades(symbol)
    trade_prices = []
    for trade in trades:
        trade_prices.append(trade["price"])
    trades_range = [max(trade_prices), min(trade_prices)]
    trades_spread = (max(trade_prices)-min(trade_prices))/min(trade_prices)
    trades_last = trade_prices[-1]
    if return_last:
        return trades_last
    return trades_range, trades_spread, trades_last

async def get_order_status(exchange, id):
    try:
        order = await exchange.fetchOrder(id)
    except:
        try:
            order = await exchange.fetchOrder(id)
        except:
            order = {}
            order["status"] = "failed"
    return order["status"]

async def get_order_by_id(exchange, id):
    try:
        return await exchange.fetchOrder(id)
    except Exception as e:
        print(e)
        return {"status":"failed", "remaining":0, "filled":0}


async def update_sell_buy_order_status(exchange, sell_order, buy_order):
    sell_status = await get_order_status(exchange, sell_order[0])
    buy_status = await get_order_status(exchange, buy_order[0])
    return sell_status, buy_status

async def loop_till_closed(exchange, symbol, order, freq, precision=4):
    order_status = await get_order_status(exchange, order[0])
    idx = 1 if order[3] == "buy" else 0
    sign = 1 if order[3] == "buy" else -1
    i = 0
    while order_status != "closed":
        last_trades = await fetch_trades_eval(exchange, symbol)
        range = last_trades[0]
        delta = round(i*(range[0]-range[1])/10,precision)
        price = range[idx] + sign * delta
        try:
            order_status = await cancel_order(exchange, order)
        except:
            order_status = await get_order_status(exchange, order[0])
        if order_status == "canceled":
            _order = [0, order[1], price, order[3]]
            order = await create_limit_order_from_list(exchange, symbol, _order)
            await asyncio.sleep(freq)
            order_status = await get_order_status(exchange, order[0])
    return order


async def cancel_order(exchange, order):
    try:
        await exchange.cancel_order(order[0])
        return "canceled"
    except:
        return "closed"

async def create_limit_order_from_list(exchange, symbol, order, add_info=None):
    from ccxt.base.errors import InsufficientFunds
    if add_info is None:
        add_info = symbol
    try:
        resp = await exchange.create_limit_order(symbol, order[3], order[1], order[2])
        return [resp["id"], order[1], order[2], order[3], add_info]
    except InsufficientFunds as err:
        return ["failed", order[1], order[2], "InsufficientFunds", add_info]
    except Exception as e:
        print(e)
        print(resp)
    return

async def get_order_size(exchange, last_price, market, value):
    min_order_size = await exchange.market(market)["precision"]["amount"]
    order_size = value/last_price if value/last_price >= min_order_size else min_order_size
    order_size = await exchange.amount_to_precision(market, order_size)
    order_size = float(order_size)
    return order_size

async def find_outbreak_target(exchange,symbol, range, timeframe="15m"):
    if not isinstance(range, list):
        range = [range, range]
    highs, lows = await get_2nd_derivate(exchange,symbol, timeframe=timeframe, limit=1000)
    high_target = ph.next_higher_in_arrays(highs, lows, range[0], treshhold=0.02)
    low_target = ph.next_lower_in_arrays(lows, lows, range[1], treshhold=0.02)
    if high_target < range[0]:
        _timeframe = timeframe
        i = ph.enumerate_timeframes(timeframe)
        while high_target < range[0]:
            if i == 0:
                print("didnt find long target in candle history setting target 5% above")
                high_target = range[0]*1.05
            else:
                print(f"i={i} timeframe = {_timeframe}")
                _timeframe = ph.shift_timeframe(_timeframe, -1)
                highs, lows = await get_2nd_derivate(exchange,symbol, timeframe=_timeframe, limit=1000)
                high_target = ph.next_higher_in_arrays(highs, lows, range[0], treshhold=0.02)
                i -= 1
    if low_target > range[1]:
        _timeframe = timeframe
        i = ph.enumerate_timeframes(timeframe)
        while low_target > range[1]:
            if i == 0:
                print("didnt find short target in candle history setting target 5% below")
                low_target = range[1]*0.95
            else:
                _timeframe = ph.shift_timeframe(_timeframe, -1)
                highs, lows = await get_2nd_derivate(exchange,symbol, timeframe=_timeframe, limit=1000)
                low_target = ph.next_lower_in_arrays(lows, lows, range[1], treshhold=0.02)
                i -= 1
    return high_target, low_target

async def create_range_orders(exchange, symbol, spread, last, buy_size, sell_size, shift_ratio):
    """:param shift_ratio [-1; 1] ... -1 sell= last, while buy= last-spread | 1 sell= last+spread, while buy= last"""

    if np.sign(shift_ratio) == 1:
        sell_shift = shift_ratio
        buy_shift = -(1-shift_ratio)
    else:
        sell_shift = 1+shift_ratio
        buy_shift = shift_ratio

    price_sell = last + sell_shift*spread
    print(f"setting prices for sell at {price_sell} / {sell_size} shift {sell_shift} and buy shift {buy_shift} according to a shift ratio of {shift_ratio}")
    price_buy = last + buy_shift*spread
    print(f"setting prices for sell at {price_buy} / {buy_size}shift {sell_shift} and buy shift {buy_shift} according to a shift ratio of {shift_ratio}")
    try:
        sell_order = await exchange.createOrder(symbol, "limit", "sell", sell_size, price_sell)
    except:
        time.sleep(1)
        sell_order = await exchange.createOrder(symbol, "limit", "sell", sell_size, price_sell)
    try:
        buy_order = await exchange.createOrder(symbol, "limit", "buy", buy_size, price_buy)
    except:
        time.sleep(1)
        buy_order = await exchange.createOrder(symbol, "limit", "buy", buy_size, price_buy)
    return [sell_order["id"],sell_order["amount"] ,sell_order["price"], "sell"], [buy_order["id"], buy_order["amount"], buy_order["price"], "buy"]

async def fetch_open_orders(exchange, symbol=None):
    open_orders = await exchange.fetch_open_orders(symbol=symbol)
    return open_orders

async def get_2nd_derivate(exchange, symbol, timeframe="1m", limit=1000, return_first_derivate=False):
    _candles = await exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit)
    candles = np.array(_candles)
    highs, lows = ph.high_low_derivate(candles)
    if return_first_derivate:
        return highs, lows
    highs, lows = ph.high_low_2nd_derivate(highs, lows)
    return highs, lows

async def next_higher_lower_in_timeframes(exchange, symbol, _price, treshhold=0.02):
    if isinstance(_price, list) or isinstance(_price, tuple):
        high_ref_price = _price[0]
        low_ref_price = _price[1]
    else:
        high_ref_price = _price
        low_ref_price = _price
    high, low = None, None
    i = 0
    while high is None or low is None:
        if i > 5:
            break
        _highs, _lows = await get_2nd_derivate(exchange,symbol, timeframe=ph.timeframes_enumerated(i), limit=1000)
        _high = ph.next_higher_in_arrays(_highs, _lows, high_ref_price, treshhold=treshhold)
        _low = ph.next_lower_in_arrays(_lows, _highs, low_ref_price, treshhold=treshhold)
        if _high > high_ref_price and high is None:
            high = _high
        if _low < low_ref_price and low is None:
            low = _low
        i += 1
    return high, low

async def get_current_range(exchange, symbol, price, treshhold=0.005):
    high, low = await next_higher_lower_in_timeframes(exchange, symbol, price, treshhold=treshhold)
    return {symbol:[high, low]}

async def get_futures_watchlist(subaccount, filter_by="volumeUsd24h", treshhold=2000000, restricted=["USDT/USD:USD"]):
    exchange = ph.initialize_exchange_driver(subaccount, init_async=True)
    markets = await exchange.load_markets()
    futures = await get_future_symbol_list(exchange)
    watch_list = []
    for future in futures:
        if float(markets[future]["info"][filter_by]) >= treshhold and not future in restricted:
            watch_list.append(future)
    await exchange.close()
    return watch_list

async def get_futures_watchlist_exchange(exchange, filter_by="volumeUsd24h", treshhold=2000000, restricted=["USDT/USD:USD"]):
    markets = await exchange.load_markets()
    futures = await get_future_symbol_list(exchange)
    watch_list = []
    for future in futures:
        if float(markets[future]["info"][filter_by]) >= treshhold and not future in restricted:
            watch_list.append(future)
    return watch_list


async def get_order_status_counter(exchange, open_orders, restricted_order_ids=[]):
    _closed_buys, _closed_sells = 0, 0
    if len(open_orders) > 0:
        for order in open_orders:
            if not order[0] in restricted_order_ids:
                try:
                    status = await get_order_status(exchange, order[0])
                except Exception as e:
                    print(e)
                    restricted_order_ids.append(order[0])
                    continue
                if status == "failed":
                    restricted_order_ids.append(order[0])
                if status == "closed":
                    restricted_order_ids.append(order[0])
                    if order[3] == "buy":
                        _closed_buys += 1
                    else:
                        _closed_sells += 1
    return _closed_buys, _closed_sells, restricted_order_ids


async def get_order_status_updates(exchange, open_orders):
    _closed_orders = []
    _canceled_orders = []
    _closed_buys, _closed_sells = 0, 0
    _failed_buy_value, _failed_buy_size = 0, 0
    _failed_sell_value, _failed_sell_size = 0, 0

    if len(open_orders) > 0:
        for order in open_orders:
            try:
                status = await get_order_status(exchange, order[0])
            except Exception as e:
                print(e)
                continue
            if status == "closed":
                _closed_orders.append(order)
                if order[3] == "buy":
                    _closed_buys += 1
                else:
                    _closed_sells += 1
            elif status == "canceled":
                _canceled_orders.append(order)
                if order[3] == "buy":
                    _failed_buy_value += order[1]*order[2]
                    _failed_buy_size += order[1]
                else:
                    _failed_sell_value += order[1]*order[2]
                    _failed_sell_size += order[1]
    return {"closed_orders":_closed_orders,
            "canceled_orders":_canceled_orders,
            "closed_buys":_closed_buys,
            "closed_sells":_closed_sells,
            "failed_buy_value":_failed_buy_value,
            "failed_buy_size":_failed_buy_size,
            "failed_sell_value":_failed_sell_value,
            "failed_sell_size":_failed_sell_size,
            }



async def gather_current_ranges_exchange(exchange, symbols=None, restricted=["USDT/USD:USD"]):
    symbols = await get_futures_watchlist_exchange(exchange)
    print("GATHER CURRENT RANGES:")
    tickers = await exchange.fetch_tickers(symbols)
    tasks = [get_current_range(exchange, symbol, value["last"]) for symbol, value in tickers.items()]
    ranges = await asyncio.gather(*tasks, return_exceptions=True)
    rd = {}
    for r in ranges:
        if isinstance(r, dict):
            for k, v in r.items():
                rd[k] = v
    print(rd)
    return rd


async def gather_current_ranges(subaccount, symbols=None, restricted=["USDT/USD:USD"]):
    exchange = ph.initialize_exchange_driver(subaccount, init_async=True)
    symbols = await get_futures_watchlist_exchange(exchange)
    print("GATHER CURRENT RANGES:")
    tickers = await exchange.fetch_tickers(symbols)
    tasks = [get_current_range(exchange, symbol, value["last"]) for symbol, value in tickers.items()]
    ranges = await asyncio.gather(*tasks, return_exceptions=True)
    await exchange.close()
    rd = {}
    for r in ranges:
        if isinstance(r, dict):
            for k, v in r.items():
                rd[k] = v
    return rd

async def cancel_all_orders_markets(exchange, symbols):
    if len(symbols) > 0:
        tasks = [can]

async def gather_current_ranges_last_hour(exchange, symbols):
    tickers = await aftx.fetch_tickers(symbols)
    tasks = [ah.get_current_range(exchange, symbol, value["last"]/(1+float(value["info"]["change1h"]))) for symbol, value in tickers.items()]
    ranges = await asyncio.gather(*tasks, return_exceptions=True)
    for r in ranges:
        if isinstance(r, dict):
            for k, v in r.items():
                rd[k] = v
    return rd

async def get_order_status_from_object(exchange, order, return_dict=True):
    if isinstance(order, list):
        id = order[0]
    else:
        id = order["id"]
    _order = await get_order_by_id(exchange, id)
    if return_dict:
        return _order
    else:
        order.append(_order["status"])
        return order


async def loop_fetch_candles(exchange, symbol, timeframe, since, till=None, limit=None, limit_per_request=5000, candles=None):
    """ :param since UST timestamp (int)"""

    if since is not None:
        since = int(since) if since > 900000000000 else int(since*1000)

    interval_s = ph.timeframe_string_to_seconds(timeframe)
    interval_ms = interval_s*1000
    max_request_ms = interval_ms*limit_per_request
    if till is not None:
        till = int(till) if till > 900000000000 else int(till*1000)
        if since is not None:
            limit = int((till - since)/interval_ms)
        else:
            since = int(till - limit*interval_ms)

    start = since
    end = since+interval_ms*limit
    end = int(time.time()*1000) if end > int(time.time()*1000) else end

    _start = start
    _end = since + max_request_ms
    while _start < end:
        _end = end if _end > end else _end
        _limit = int((_end-_start)/interval_ms)
        if _limit > 0:
            _candles = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=_start, limit=_limit)
            _candles = np.array(_candles)
            if candles is None:
                candles = _candles
            else:
                candles = np.append(candles, _candles, axis=0)
        else:
            break
        _start = _end
        _end += max_request_ms
    return candles

class RunThread(threading.Thread):
    def __init__(self, func, args, kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        super().__init__()

    def run(self):
        if self.args is None and self.kwargs is None:
            self.result = asyncio.run(self.func())
            return self.result
        self.result = asyncio.run(self.func(*self.args, **self.kwargs))
        return self.result