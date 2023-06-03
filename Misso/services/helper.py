import uuid
import json
import numpy as np
import pandas as pd
import ccxt
import os
import time

parse_side = dict(long="buy", short="sell", buy="buy", sell="sell")
parse_exit_side = dict(long="sell", short="buy", buy="sell", sell="buy")

def price_in_range(price: float, range: list, buffer: float=0):
    delta = abs(range[0] - range[1]) * buffer
    return price >= range[0]-delta and price <= range[1]+delta

def get_latest_json(prefix: str, dir: str):
    files = [f for f in os.listdir(dir) if f.startswith(prefix)]
    if len(files) == 0:
        return None
    files.sort()
    with open(f"{dir}/{files[-1]}", "r") as f:
        data = json.load(f)
    return data

def safe_parse_side(response):
    parse_side = dict(long="buy", short="sell", buy="buy", sell="sell")
    return parse_side[response["side"]]

def safe_parse_exit_side(response):
    parse_side = dict(long="sell", short="buy", buy="sell", sell="buy")
    return parse_side[response["side"]]

def safe_parse_break_even(response):
    try:
        break_even = float(response["info"]["recentBreakEvenPrice"])
    except:
        break_even = response["entryPrice"]
    return break_even


def get_path_prefix(main="Misso"):
    p = os.getcwd()
    i = ""
    while os.path.split(p)[1] != main:
        i += "..\\"
        p = os.path.split(p)[0]
    return i, p

def fetch_save_market_states(counter):
    from Misso import watch_list
    from Misso.market_states import marketWatcher
    ms = marketWatcher.get_states(watch_list)
    path = save_to_json_dict(ms, file=f"market_states_{counter}.json", dir="storage")
    return ms, path


def execute_asyncio(method, args, kwargs):
    import asyncio
    from Misso.services.async_helper import RunThread
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # 'RuntimeError: There is no current event loop...'
        loop = None
    if loop is not None and loop.is_running():
        thread = RunThread(method, args, kwargs)
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.get_event_loop().run_until_complete(method(*args, **kwargs))

def increment_price_up(exchange, price, symbol, n_ticks=1):
    return float(exchange.price_to_precision(symbol, price)) + n_ticks * exchange.markets[symbol]["precision"]["price"]

def increment_price_down(exchange, price, symbol, n_ticks=1):
    return float(exchange.price_to_precision(symbol, price)) - n_ticks * exchange.markets[symbol]["precision"]["price"]

def increment_by_side(exchange, symbol, side="buy", n_ticks=1):
    sign = 1 if side == "buy" else -1
    return n_ticks*exchange.markets[symbol]["precision"]["price"]*sign

def increment_price_by_side(exchange, symbol, price, side, n_ticks=1):
    return float(exchange.price_to_precision(symbol, price)) + increment_by_side(exchange, symbol, side, n_ticks)

def safe_last_price(exchange, symbol, last=None):
    if not last:
        try:
            last = exchange.fetch_ticker(symbol)["last"]
        except:
            time.sleep(0.2)
            _exchange = ccxt.ftx()
            last = _exchange.fetch_ticker(symbol)["last"]
    return last

def _as_array(arr):
    if isinstance(arr, np.ndarray):
        return arr
    return np.array(arr)

def safe_merge_waves(lows, highs):
    min_dist = abs(lows[0] - lows[1])/2
    ls = np.sort(np.concatenate((lows, highs)))
    _ls = np.roll(ls, -1)
    return ls[np.logical_or(_ls - ls>min_dist, _ls - ls<0)]

def ar_by_col(ar: np.array, col: str):
    d = {'Date': 0, 'Open': 1, 'High': 2, 'Low': 3, 'Close': 4, 'Volume': 5}
    return ar[:,d[col]]

def get_wave_target_pairs(market_state: dict, timeframe:str=None, buy_waves=None, sell_waves=None, by="next"):
    _market_state = safe_from_dict(market_state, timeframe, market_state)
    buy_waves = safe_from_dict(_market_state, {"data":"low_waves"}, buy_waves)
    sell_waves = safe_from_dict(_market_state, {"data":"high_waves"}, sell_waves)
    wp = {"buy":{}, "sell":{}}
    if not buy_waves is None and not sell_waves is None:
        merged_waves = safe_merge_waves(buy_waves, sell_waves)
        min_dist = (abs(merged_waves[0] - merged_waves[1])/merged_waves[0])/2
        buy_waves = -np.sort(-_as_array(buy_waves))
        _w = -np.sort(-_as_array(sell_waves))
        for i, w in enumerate(buy_waves):
            if by == "index":
                wp["buy"][i] = (w, _w[i])
            else:
                wp["buy"][i] = (w, next_higher_in_array(merged_waves, w, treshhold=min_dist))
        sell_waves = np.sort(_as_array(sell_waves))
        _w = np.sort(_as_array(buy_waves))
        for i, w in enumerate(sell_waves):
            if by == "index":
                wp["sell"][i] = (w, _w[i])
            else:
                wp["sell"][i] = (w, next_lower_in_array(merged_waves, w, treshhold=min_dist))
    if timeframe:
        return {timeframe:wp}
    return wp

def get_watch_list_from_positions(subaccount):
    ftx = initialize_exchange_driver(subaccount)
    positions = ftx.fetch_positions()
    wl = []
    for pos in positions:
        wl.append(pos["symbol"])
    return wl

def is_free_balance_for_order(order: list, free_balance: float, ticker_price: float, leverage: int=20):
    return free_balance > (order[1] * ticker_price) / leverage

def is_free_balance_for_opening(target_value, free_balance, leverage=20):
    return free_balance > 2*target_value/leverage

def find_markets_by_abs(exchange: ccxt, min_volume: float=500000, skip=["USDT/USD"], max_order_value: float=5, min_abs: float=None, futures_only: bool=True, symbols_only: bool= False):
    if exchange is None:
        exchange = ccxt.ftx()
    ticker_list = []
    ticker = exchange.fetch_tickers()
    for key, value in ticker.items():
        try:
            if float(value["info"]['volumeUsd24h']) > min_volume and key not in skip and not value["info"]["restricted"]:
                if futures_only:
                    if value["info"]["type"] != "future":
                        continue
                if float(value["info"]['minProvideSize'])*value["last"] < max_order_value:
                    spread = abs(value["ask"] - value["bid"])/value["last"]
                    if min_abs:
                        if spread >= min_abs:
                            ticker_list.append(key)
                    else:
                        ticker_list.append((key, spread))
        except:
            continue
    if min_abs:
        return ticker_list, ticker
    res = sort_tuple_list(ticker_list, keys_only=symbols_only)
    return res

def sort_tuple_list(tuple_list: list, key_index: int=1, descending: bool=True, keys_only: bool=False):
    def takeKey(elem):
        return elem[key_index]
    if descending:
        tuple_list.sort(key=takeKey, reverse=True)
    else:
        tuple_list.sort(key=takeKey)
    return [key for key, _ in tuple_list] if keys_only else tuple_list

def change_sub(exchange, subaccount):
    from Misso.config import API_CONFIG
    exchange = exchange or initialize_exchange_driver("Main")
    if subaccount in API_CONFIG:
        exchange.headers["FTX-SUBACCOUNT"] = subaccount
        return exchange
    else:
        print("subaccount not in exchange_config.yaml")

def get_exit_order_in_profit(response: dict, profit_target: float, exchange: ccxt=None, assert_profit:bool =True):
    pos_side = parse_side[response["side"]]
    exit_side = parse_exit_side[response["side"]]
    price = float(response["info"]["recentBreakEvenPrice"])
    price = price * (1 + profit_target) if pos_side == "buy" else price * (1 - profit_target)
    price = price if exchange is None else float(exchange.price_to_precision(response["symbol"], price))
    if assert_profit:
        while not price_in_profit(pos_side, float(response["info"]["recentBreakEvenPrice"]), price):
            price = price * (1 + profit_target) if pos_side == "buy" else price * (1 - profit_target)
            price = price if exchange is None else float(exchange.price_to_precision(response["symbol"], price))
            profit_target += 0.001
    return [0, response["contracts"], price, exit_side, response["symbol"]]

def get_exit_order_at_last(response: dict, exchange: ccxt, last: float=None):
    exit_side = parse_exit_side[response["side"]]
    price = safe_last_price(exchange, response["symbol"], last)
    return [0, response["contracts"], price, exit_side, response["symbol"]]

def set_closing_orders(subaccount: str, profit_target: float=0.005, cancel_all: bool=False):
    import time
    ftx = initialize_exchange_driver(subaccount)
    if cancel_all:
        ftx.cancel_all_orders()
    pos = ftx.fetch_positions()
    orders = []
    for p in pos:
        if p["contracts"] > 0:
            orders.append(get_exit_order_in_profit(p, profit_target, ftx))
    for o in orders:
        try:
            create_limit_order(ftx, o)
            time.sleep(0.2)
        except Exception as err:
            print(f"ERROR occured with {o[4]}:")
            print(err)

def dca_all_positions(subaccount: str, min_range: float=0.005, cancel_all=True, side="long"):
    ftx = initialize_exchange_driver(subaccount)
    if cancel_all:
        ftx.cancel_all_orders()
    pos = ftx.fetch_positions()
    for p in pos:
        if p["contracts"] > 0 and p["side"] == side:
            order = get_dca_order(ftx, p, min_range)
            try:
                create_limit_order(ftx, order)
            except Exception as err:
                print(f"ERROR occured with {order[4]}:")
                print(err)

def get_dca_order(exchange: ccxt, response: dict, min_range: float):
    side = parse_side[response["side"]]
    price = get_next_dca_price(exchange, response["symbol"], side, min_range)
    price = float(exchange.price_to_precision(response["symbol"], price))
    return [0, response["contracts"], price, side, response["symbol"]]

def get_next_dca_price(exchange: ccxt, symbol: str, side: str, min_delta: float):

    last_dca_level = exchange.fetch_ticker(symbol)["last"]
    next_delta = min_delta
    next_range = get_dca_levels(exchange, symbol, last_dca_level, side, 1, init_treshhold=next_delta)
    high = max(next_range)
    low = min(next_range)
    low = last_dca_level * (1-next_delta) if low is None else low
    high = last_dca_level * (1+next_delta) if high is None else high
    if side == "buy":
        return float(exchange.price_to_precision(symbol, low))
    return float(exchange.price_to_precision(symbol, high))


def price_in_profit(side: str, break_even: float, price: float):
    if side == "buy":
        return break_even < price
    return break_even > price



def get_position_update(exchange: object, symbol: str, key: str=None):
    positions = exchange.fetch_positions()
    for pos in positions:
        if pos["symbol"] == symbol:
            if key is None:
                return pos
            return pos[key]

def reunify_watch_list(watch_list: list):
    wl = []
    for sym in watch_list:
        wl.append(reunify_symbol(sym))
    return wl

def unify_symbol(symbol: str):
    if symbol.split("-")[1] == "PERP":
        return symbol.split("-")[0]+"/USD:USD"
    else:
        return symbol

def reunify_symbol(uni_symbol: str):
    if uni_symbol.split("/")[1] == "USD:USD":
        return uni_symbol.split("/")[0] + "-PERP"

def generate_Positions_from_open_positions(exchange, target_value):
    """ USED IN SWING SLOW STRATEGY"""
    from Misso.models.draft.swing_slow import Positions
    open_pos = exchange.fetch_positions()
    open_pos = open_positions_to_dict(open_pos)
    symbols = list(open_pos.keys())
    params = get_unit_params(exchange, symbols, target_value)
    positions_store = Positions(params, target_value)
    positions_store.parse_open_positions(open_pos)
    return positions_store

#for position_single_order
def position_has_closed_orders(position):
    if position.buy_status in ["closed", "canceled", "failed"]:
        return True
    if position.sell_status in ["closed", "canceled", "failed"]:
        return True
    return False
# single_order position dataclass
def dca_not_set(position):
    if position.side == "buy":
        return position.buy_status != "open"
    if position.side == "sell":
        return position.sell_status != "open"

def dca_order_filled(position):
    if position.side == "buy":
        return position.buy_status == "closed"
    if position.side == "sell":
        return position.sell_status == "closed"

# single_order position dataclass
def assert_exit_size(position):
    if position.side == "buy":
        if position.sell_status == "open":
            return position.size <= position.sell_order[1]
    if position.side == "sell":
        if position.buy_order == "open":
            return position.size <= position.buy_order[1]
    return False

# single order position dataclass
def exit_not_set(position):
    if position.side == "sell":
        return position.buy_status != "open"
    if position.side == "buy":
        return position.sell_status != "open"


def open_positions_to_dict(response):
    positions = {}
    for pos in response:
        if pos["contracts"] > 0:
            positions[pos["symbol"]] = pos
    return positions

def get_unit_params(exchange: object, watch_list: list, target_value: float):
    """Declares default unit size (order size) based on capital and exchange precision"""
    exchange.load_markets()
    d = {}
    for k in watch_list:
        v = exchange.markets[k]
        min_size = v["limits"]["amount"]["min"]
        unit_size = target_value / float(v["info"]["last"])
        unit_size = min_size if unit_size < min_size else unit_size
        unit_size = float(exchange.amount_to_precision(k, unit_size))
        d[k] = {"unit_size":unit_size, "ref_price":float(v["info"]["last"])}
    return d


def get_unit_param(exchange: object, symbol: str, target_value: float, return_size_only: bool=False):
    if exchange is None:
        import ccxt
        exchange = ccxt.ftx()
    exchange.load_markets()
    v = exchange.markets[symbol]
    min_size = v["limits"]["amount"]["min"]
    unit_size = target_value / float(v["info"]["last"])
    unit_size = min_size if unit_size < min_size else unit_size
    unit_size = float(exchange.amount_to_precision(symbol, unit_size))
    if return_size_only:
        return unit_size
    return {"unit_size":unit_size, "ref_price":float(v["info"]["last"])}


#########  Fisso strategy helper
#

def open_position(exchange: object, closed_positions: list, tickers: dict, candles: dict, min_range_open: float, order_value: float):
    for pos in closed_positions:
        market = pos.symbol
        range = abs(tickers[market]["bid"] - tickers[market]["ask"])
        min_max = [candles[market][-1, 3], candles[market][-1, 2]]
        spread = range/tickers[market]["ask"]
        price = tickers[market]["last"]
        avg_price =np.mean(abs(candles[market][-5:,4] + candles[market][-5:,1])/2)

        size = get_order_size(exchange, price, market, order_value)
        buy_prices, sell_prices = [], []
        orders = []

        if range > pos.price_increment and spread > min_range_open:
            if price > avg_price: # condition 1
                #action 1
                buy_price = tickers[market]["bid"] + pos.price_increment
                buy_prices.append(buy_price)
                #action 7
                sell_price = tickers[market]["ask"] + pos.price_increment
                sell_prices.append(sell_price)
                print("condition 1")
            if price < avg_price: # condition 2
                #action 2
                buy_price = tickers[market]["bid"] - pos.price_increment
                buy_prices.append(buy_price)
                #action 6
                sell_price = tickers[market]["ask"] - pos.price_increment
                sell_prices.append(sell_price)
                print("condition 2")
        if abs(min_max[0] - min_max[1])/price > min_range_open:
            if price > avg_price: # condition 3
                #action 3
                buy_price = min_max[0] + pos.price_increment
                buy_prices.append(buy_price)
                #action 9
                sell_price = min_max[1] * (1+min_range_open)
                sell_prices.append(sell_price)
                print("condition 3")
            if price < avg_price: # condition 4
                #action 4
                buy_price = min_max[0] * (1-min_range_open)
                buy_prices.append(buy_price)
                #action 8
                sell_price = min_max[1] - pos.price_increment
                sell_prices.append(sell_price)
                print("condition 4")
        if len(buy_prices) == 0: # condition b
            #action 5
            buy_price = price * (1-min_range_open*1.8)
            buy_prices.append(buy_price)
            print("condition b")
        if len(sell_prices) == 0: # condition s
            #action 10
            if abs(min_max[0] - min_max[1])/price > min_range_open:
                sell_price = price * (1+(abs(min_max[0] - min_max[1])/price)/2)
            else:
                sell_price = price * (1+min_range_open*1.8)
            sell_prices.append(sell_price)
            print("condition s")

        sell_orders = [[0, size, exchange.price_to_precision(market, price), "sell", market] for price in sell_prices]
        buy_orders = [[0, size, exchange.price_to_precision(market, price), "buy", market] for price in buy_prices]
        orders = orders + sell_orders + buy_orders
    return orders



#########  Position dataclass helper
#
def find_positions_by_condition(positions: list, attribute:str, value:any):
    return list(filter(lambda pos: getattr(pos, attribute) == value, positions))

def find_positions_by_not_condition(positions: list, attribute:str, value:any):
    return list(filter(lambda pos: getattr(pos, attribute) != value, positions))

#########  Order dataclass helper
#

def parse_is_open_orders(response):
    d = {}
    for o in response:
        d[o["symbol"]][o["id"]] = o
    return d

def find_orders_by_symbol(open_orders: list, symbol: str):
    return list(filter(lambda order: order.symbol == symbol, open_orders))

def find_processed_orders(open_orders: list, is_processed: bool):
    return list(filter(lambda order: order.is_processed == is_processed, open_orders))

def find_orders_by_status(open_orders: list, status: str):
    return list(filter(lambda order: order.status == status, open_orders))

def find_orders_by_not_status(open_orders: list, not_status: str):
    return list(filter(lambda order: order.status != not_status, open_orders))

def find_orders_by_side(open_orders: list, side: str):
    return list(filter(lambda order: order.side == side, open_orders))

def find_recently_updated_orders(open_orders: list, is_updated: bool):
    return list(filter(lambda order: order.is_updated == is_updated, open_orders))

def order_in_is_open_orders(order: object, is_open_orders: dict):
    if order.symbol not in is_open_orders:
        return False
    elif order.id not in is_open_orders[order.symbol]:
        return False
    return True

def find_open_positions(positions: list, bool_target=True):
    return list(filter(lambda pos: pos.is_open == bool_target, positions))

def pos_list_to_dict(pos_list, as_objects=True):
    d = {}
    for pos in pos_list:
        d[pos.symbol] = pos if as_objects else pos.return_dict()
    return d

def reset_position_tmpl_dict():
    d = {'size': 0.0,
         'cost': 0.0,
         'value': 0.0,
         'break_even': 0.0,
         'avg_price': 0.0,
         'side': None,
         'is_open': False,
         'is_pending': False,
         'dca_count': 0,
         'dca_levels': [],
         'exit_target': 0.0,
         'current_exit_size': 0.0,
         'dca_order_set': False,
         'exit_order_set': False
         }
    return d
#########

def formatted_positions_update(live_pos, open_pos_stack=None):
    d = {}
    for symbol, value in live_pos.items():
        d[symbol] = formatted_position_update(value)
    if open_pos_stack is not None:
        for symbol in open_pos_stack.keys():
            if symbol not in d:
                d[symbol] = reset_position_tmpl_dict()
    return d

def changes_position_update(before, after):
    d = {}
    is_change = False
    for key, value in after.items():
        if value != before[key]:
            is_change = True
            d[key] = {"before":before[key], "after":value}
    return d, is_change

def formatted_position_update(live_pos):
    d = {}
    d["size"] = live_pos["contracts"]
    d["cost"] = live_pos["notional"]
    d["break_even"] = float(live_pos["info"]["recentBreakEvenPrice"])
    d["avg_price"] = live_pos["entryPrice"]
    d["side"] = live_pos["info"]["side"]
    d["is_open"] = True
    d["is_pending"] = False
    return d

def extend_list_by_dict_keys(target_list, source_dict):
    for key in source_dict:
        if key not in target_list:
            target_list.append(key)
    return target_list

def get_nested_dict_value(nested_dict, dot_key):
    """ gets value from dict by multilevel keystring from nested_dict. e.g.:
    d={persons:{person1:{name:{surname:Max, famname:Mustermann}, age: 22}, person2:{...}
     dot_key_string: persons.person1.name.surname  => access surname of person1"""
    if "." in dot_key:
        value = nested_dict
        for k in dot_key.split("."):
            value = value[k]
    else:
        value = nested_dict[dot_key]
    return value

def change_type_by_string(value, target_type):
    if target_type == "float":
        return float(value)
    if target_type == "str" or target_type =="string":
        return str(value)
    if target_type == "int" or target_type == "integer":
        return int(value)

def filter_dict_by_config(input_dict, filter_config):
    for key, condition in filter_config.items():
        value = get_nested_dict_value(input_dict, key)
        if "value_type" in condition:
            value = change_type_by_string(value, condition["value_type"])

        if condition["type"] == "smaller_or_equal":
            if not value <= condition["value"]:
                return False
        if condition["type"] == "smaller":
            if not value < condition["value"]:
                return False
        if condition["type"] == "greater_or_equal":
            if not value >= condition["value"]:
                return False
        if condition["type"] == "greater":
            if not value > condition["value"]:
                return False
        if condition["type"] == "equal":
            if not value == condition["value"]:
                return False
    return True

def get_future_symbol_list_from_markets_dict(markets: dict):
    symbol_list = []
    for m in markets:
        if m.endswith("USD:USD"):
            symbol_list.append(m)
    return symbol_list

def get_watch_list_from_wl_ranges(wl_ranges, restricted):
    watch_list = []
    for wl in list(wl_ranges.keys()):
        if wl not in restricted:
            watch_list.append(wl)
    return watch_list

def evaluate_system_status(system_status_resp, error_logger=None, return_down=True, return_up=False):
    if error_logger is None:
        from Misso.services.logger import _init_error_logger
        error_logger = _init_error_logger()
    pinged_status = system_status_resp.copy()
    down = []
    up = []
    for name, status in pinged_status.items():
        if isinstance(status, bool):
            if status:
                up.append(name)
            else:
                down.append(name)
        else:
            down.append(name)

    if return_down:
        if len(down) > 0:
            telegram_notify_system_status(down, error_logger)
    if return_up:
        if len(up) > 0:
            telegram_notify_working_systems(up, error_logger)

def telegram_notify_msg(msg, error_logger):
    msg = telegram_format_msg(msg)
    telegram_notify(msg, error_logger)

def telegram_notify_system_status(down_list, error_logger) -> None:
    msg = f"{len(down_list)} Moduls Down:"
    for down in down_list:
        msg += f"\n .... {down}"
    msg = telegram_format_msg(msg)
    telegram_notify(msg, error_logger)

def telegram_notify_working_systems(up_list, error_logger) -> None:
    msg = f"{len(up_list)} Moduls Running Good"
    for up in up_list:
        msg += f"\n .... {up}"
    msg = telegram_format_msg(msg)
    telegram_notify(msg, error_logger)

def set_value_type(value, _type):
    if _type == "bool":
        if value == "False":
            return False
        else:
            return True
    if _type == "int":
        return int(value)
    if _type == "float":
        return float(value)
    if _type == "str":
        return str(value)
    if _type == "list":
        value = value[1:]
        value = value[:-1]
        return [i for i in value.split(",")]
    if _type == "list_float":
        value = value[1:]
        value = value[:-1]
        return [float(i) for i in value.split(",")]
    if _type == "list_int":
        value = value[1:]
        value = value[:-1]
        return [int(i) for i in value.split(",")]
    if _type == "list_bool":
        value = value[1:]
        value = value[:-1]
        ls =  [i for i in value.split(",")]
        print(ls)
        res = []
        for l in ls:
            if "," in l:
                l.replace(",", "")
            if l == "True":
                res.append(True)
            else:
                res.append(False)
        return res


def telegram_notify(msg, error_logger=None, token=None, chat_id=None, reformat=True) -> None:
    import requests
    if token is None:
        token = ""
    if chat_id is None:
        chat_id = ""
    if reformat:
        msg = telegram_format_msg(msg)
    print("TELEGRAM LOGGER: ##################\n\n")
    print(msg)
    try:
        response = requests.get(
            f'https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&parse_mode=Markdown&text={msg}'
        )
        if response.status_code // 100 != 2:
            err_msg = f'Telegram error [{response.status_code}]: {response.text}'
            print(err_msg)
            if response.status_code // 100 == 4:
                err_msg += f'\nParameters: {msg}'
            if not error_logger is None:
                error_logger.error(err_msg)
    except requests.exceptions.ConnectionError:
        print('Telegram error: ConnectionError')
        if not error_logger is None:
            error_logger.error('Telegram error: ConnectionError')


def telegram_format_msg(msg: str) -> str:
    msg = msg.replace('_', '\_')
    msg = msg.replace('*', '\*')
    msg = msg.replace('[', '\[')
    #msg = msg.replace(']', '\]')
    if len(msg) > 2000:
        return msg[-2000:]
    return msg

def yaml_to_dict(path = "exchange_config.yaml"):
    import yaml
    with open(path) as f:
        dict = yaml.load(f, Loader=yaml.FullLoader)
    return dict

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %X')
        return json.JSONEncoder.default(self, obj)

def save_to_json_dict(state_dict, file="state_dict.json", dir=None):
    import json
    import os
    import datetime
    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.bool_):
                return bool(obj)
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            if isinstance(obj, pd.Timestamp):
                return obj.strftime('%Y-%m-%d %X')
            if isinstance(obj, datetime.datetime):
                return obj.strftime('%Y-%m-%d %X')
            return json.JSONEncoder.default(self, obj)
    if not dir is None:
        file = os.path.join(dir, file)
    with open(file, 'w') as fp:
        json.dump(state_dict, fp, cls=NpEncoder)
    return file

def load_from_json(file):
    import json

    with open(file, "r") as f:
        _dict = json.load(f)
    return _dict




def get_decimals(number):
    import decimal
    d = decimal.Decimal(str(number))
    return int(abs(d.as_tuple().exponent))

def makedir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def generate_unique_id() -> str:
    return str(uuid.uuid4())

def type_to_side(t: str) -> str:
    if t == "long":
        return "buy"
    if t == "short":
        return "sell"
    raise ValueError(f'unsupported type: "{t}". Only "long" and "short" are supported.')

def opposite_side(side:str):
    if side == "buy":
        return "sell"
    if side == "sell":
        return "buy"
    return "buy"

def np_shift(arr: np.ndarray, num: int, fill_value=0) -> np.ndarray:
    result = np.empty_like(arr)

    if num > 0:
        result[:num] = fill_value
        result[num:] = arr[:-num]
    elif num < 0:
        result[num:] = fill_value
        result[:num] = arr[-num:]
    else:
        result[:] = arr

    return result

def str_to_timestamp(string):
    return int(pd.to_datetime(string).timestamp())

def is_in_dict(key, dict):
    return key in dict

def config_or_default(key, dict, default):
    if is_in_dict(key, dict):
        return dict[key]
    return default

def config_or_none(key, dict):
    if is_in_dict(key, dict):
        return dict[key]
    return None

def config_or_false(key, dict):
    if is_in_dict(key, dict):
        return bool(dict[key])
    return False

def in_dict_not_none(key, search_dict, replace_none=0):
    if isinstance(search_dict, dict):
        if key in search_dict:
            if search_dict[key] is not None:
                return search_dict[key]
        #print(f"{search_dict} is not type dict returnin 0 for search key {key}")
    return replace_none

def in_dict_not_none_pos_side(key, search_dict, replace_none=0):
    if isinstance(search_dict, dict):
        if key in search_dict:
            if search_dict[key] is not None:
                if search_dict[key] == "short" or search_dict[key] == "sell":
                    return "sell"
                if search_dict[key] == "long" or search_dict[key] == "buy":
                    return "buy"
                return "sell" if search_dict[key] == "short" else "buy"
        #print(f"{search_dict} is not type dict returnn buy for search key {key}")
    return replace_none

def formatted_orderbook(response):
    asks = np.array(response["result"]["asks"])
    bids = np.array(response["result"]["bids"])
    return asks, bids

def lowest_ask(asks):
    return asks[0,0]

def highest_bid(bids):
    return bids[0,0]

def value_to_qty(size, price):
    return size/price

def generate_parameter(**kwargs):
    return {**kwargs}

def current_time():
    return pd.to_datetime(int(time.time()), unit="s")

def str_to_datetime(str):
    return pd.to_datetime(str).replace(tzinfo=None)

def unify_side(side):
    if side == "short" or "sell":
        return "sell"
    if side == "long" or "buy":
        return "buy"
    else:
        return None

def unify_symbol(symbol):
    if symbol.split("-")[1] == "PERP":
        return symbol.split("-")[0]+"/USD:USD"
    else:
        return symbol

def reunify_symbol(uni_symbol):
    if uni_symbol.split("/")[1] == "USD:USD":
        return uni_symbol.split("/")[0] + "-PERP"


def safename(symbol):
    symbol = symbol.replace("/","-")
    return symbol.replace(":","_")


def make_jsonfile(path):
    import json
    data = [{"logs_version":1}]
    with open(path, "w") as f:
        json.dump(data, f)
    return path

def update_json_by_one(path, log, key="logs"):
    import json
    import os

    if not os.path.exists(path):
        make_jsonfile(path)
    try:
        with open(path, "r") as f:
            data = json.load(f)
        if type(data) is dict:
            data = [data]
        data.append(log)
        with open(path, "w") as sourcefile:
            json.dump(data, sourcefile)
    except:
        os.remove(path)
        make_jsonfile(path)
        with open(path, "r") as f:
            data = json.load(f)
        if type(data) is dict:
            data = [data]
        data.append(log)
        with open(path, "w") as sourcefile:
            json.dump(data, sourcefile)

def update_json(path, new_measurements=[], key="logs"):
    #print(f"update json {path}")
    import json
    if len(new_measurements) > 0:
        with open(path, "r") as f:
            data = json.load(f)
        if type(data) is dict:
            data = [data]

        for log in new_measurements:
            data.append(log)

        with open("out_test.json", "w") as outfile:
            json.dump(data, outfile)

        with open(path, "w") as sourcefile:
            json.dump(data, sourcefile)


def is_dict_not_class(obj, target_class):
    if isinstance(obj, dict):
        return target_class(obj)

def order_to_dict_universal(order):
    if not isinstance(order, dict):
        order = order.to_dict()
    return order

def find_value_for_key(dict, key):
    if key in ["amount","size","qty"]:
        return dict[try_div_qty_strings(dict)]
    if key in dict.keys():
        return dict[key]
    elif key in dict["info"].keys():
        return dict["info"][key]
    return None

def find_volatile_market(min_market_volume, restricted=[], return_first=0):
    import Misso.services.async_helper as ah
    import asyncio
    markets = asyncio.run(ah.get_volatile_markets(min_market_volume))
    if len(restricted) > 0:
        filtered_markets = []
        for market in markets:
            if not market in restricted:
                filtered_markets.append(market)
        markets = filtered_markets
    if return_first > 0:
        return markets[:return_first]
    return markets

def get_open_position_symbol(exchange, mode="highest"):
    import numpy as np
    positions = exchange.fetch_positions()
    open = []
    for pos in positions:
        if pos["contracts"] > 0:
            open.append([pos["symbol"], pos["notional"]])
    if len(open) > 0:
        a = np.array(open)
        idx = np.argmax(a[:,1], axis=0)
        return a[idx,0]
    return None

def try_div_qty_strings(dict):
    if "amount" in dict:
        return "amount"
    if "size" in dict:
        return "size"
    if "qty" in dict:
        return "qty"

def get_order_main_feature(order, features):
    order = order_to_dict_universal(order)
    feature_tuple = (
        find_value_for_key(order, features[1]),
        find_value_for_key(order, features[2]),
        find_value_for_key(order, features[3])
    )
    return order["id"], feature_tuple

def open_positions(exchange, return_symbols=False):
    ps = exchange.fetch_positions()
    res = []
    pos = []
    for _p in ps:
        if _p["contracts"] > 0:
            res.append(_p["symbol"])
            pos.append(_p)
    if return_symbols:
        return res
    return pos

def parse_config_yaml(yaml_path):
    import yaml
    with open(yaml_path) as f:
        attr_dict = yaml.load(f, Loader=yaml.FullLoader)
    return attr_dict

def df_to_dict_value(df, key, symbol):
    return {key:float(df[key][df["symbol"] == symbol])}

def candles_to_df(candles, columns=["Date", "Open","High","Low","Close","Volume"]): #["date", "open","high","low","close","volume"]
    df = pd.DataFrame(candles, columns=columns)
    df[columns[0]] = pd.to_datetime(df[columns[0]], unit="ms")
    return df


def spread_from_candles(candles):
    if isinstance(candles, list):
        import numpy as np
        candles = np.array(candles)
    _spread = (candles[:,2] - candles[:,3])/candles[:,3]
    avg_spread = _spread.mean()
    return avg_spread

def spread_from_candles_abs(candles):
    _spread = (candles[:,2] - candles[:,3])
    avg_spread = _spread.mean()
    return avg_spread

def avg_volume_from_candles(candles):
    if isinstance(candles, list):
        import numpy as np
        candles = np.array(candles)
    return candles[:,5].mean()

def range_from_candles(candles):
    _max = candles[:,2].max()
    _min = candles[:,3].min()
    return [_max, _min]

def range_spread_ratio(range, spread):
    _diff = range[0] - range[1]
    return _diff/spread

def get_candle_eval_dict(exchange, symbol, key=None, timeframe="1m", limit=5):
    unit_dict = {}
    try:
        candles = np.array(exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit))
    except:
        time.sleep(1)
        candles = np.array(exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit))
    unit_dict["candles"] = candles
    unit_dict["avg_spread"] = spread_from_candles(candles)
    unit_dict["avg_spread_abs"] = spread_from_candles_abs(candles)
    unit_dict["range"] = range_from_candles(candles)
    unit_dict["range_spread_ratio"] = range_spread_ratio(unit_dict["range"], unit_dict["avg_spread_abs"])
    if key is not None:
        return unit_dict[key]
    return unit_dict

def get_future_symbol_list(exchange):
    markets = exchange.fetch_markets()
    symbol_list = []
    for m in markets:
        if m["symbol"].endswith("USD:USD"):
            symbol_list.append(m["symbol"])
    return symbol_list

def get_subaccounts():
    from Misso.config import API_CONFIG
    return list(API_CONFIG.keys())

def get_current_subaccount(exchange: ccxt):
    return exchange.headers["FTX-SUBACCOUNT"]

def get_exchange_config_for_subbaccount(subaccount):
    from Misso.config import API_CONFIG
    return API_CONFIG[subaccount]

def init_exchange_driver_from_config(config=None):
    import ccxt
    ftx = ccxt.ftx()
    sub = config["subaccount"]
    ftx.apiKey = config["api_key"]
    ftx.secret = config["api_secret"]
    if sub != "Main":
        ftx.headers = {"FTX-SUBACCOUNT":sub}
    ftx.load_markets()
    return ftx

def init_exchange_driver_from_config_async(config=None):
    import ccxt.async_support as ccxt
    ftx = ccxt.ftx()
    sub = config["subaccount"]
    ftx.apiKey = config["api_key"]
    ftx.secret = config["api_secret"]
    ftx.headers = {"FTX-SUBACCOUNT":sub}
    return ftx

async def close_exchange(exchange):
    await exchange.close()

def initialize_exchange_driver(subaccount, init_async=False, exchange=None):
    import asyncio
    if exchange is not None:
        asyncio.run(close_exchange(exchange))
    config = get_exchange_config_for_subbaccount(subaccount)
    if init_async:
        return init_exchange_driver_from_config_async(config)
    else:
        return init_exchange_driver_from_config(config)

def get_highs_lows_from_candles(candles):
    highs, _ = get_high_lows(candles[:,2])
    _, lows = get_high_lows(candles[:,3])
    return highs, lows


def get_high_lows(array):
    a = np.diff(array)
    n = np.sign(a)
    a = np.roll(n, shift=1)
    highs = []
    lows = []
    for i in range(len(a)):
        if i == 0:
            highs.append(0)
            lows.append(0)

        elif a[i] == 1 and n[i] != 1:
            highs.append(1)
            lows.append(0)
        elif a[i] == -1 and n[i] != -1:
            highs.append(0)
            lows.append(1)
        else:
            highs.append(0)
            lows.append(0)
    high_prices = array[np.where(np.array(highs) == 1)]
    low_prices = array[np.where(np.array(lows) == 1)]
    return high_prices, low_prices

def order_list_from_response(response):
    if "id" in response:
        return [response["id"], response["amount"], response["price"], response["side"]]
    else:
        return ["failed", 0, 0, 0]

def create_range_orders(exchange, symbol, spread, last, buy_size, sell_size, shift_ratio):
    """:param shift_ratio [-1; 1] ... -1 sell= last, while buy= last-spread | 1 sell= last+spread, while buy= last"""
    if shift_ratio > 1:
        shift_ratio = 1
    if shift_ratio < -1:
        shift_ratio = -1
    if np.sign(shift_ratio) == 1:
        sell_shift = shift_ratio
        buy_shift = -(1-shift_ratio)
    else:
        sell_shift = 1+shift_ratio
        buy_shift = shift_ratio

    price_sell = last + sell_shift*spread
    #print(f"setting prices for sell at {price_sell} / {sell_size} shift {sell_shift} and buy shift {buy_shift} according to a shift ratio of {shift_ratio}")
    price_buy = last + buy_shift*spread
    #print(f"setting prices for sell at {price_buy} / {buy_size}shift {sell_shift} and buy shift {buy_shift} according to a shift ratio of {shift_ratio}")
    if sell_size > 0:
        try:
            sell_order = exchange.createOrder(symbol, "limit", "sell", sell_size, price_sell)
        except:
            time.sleep(1)
            try:
                sell_order = exchange.createOrder(symbol, "limit", "sell", sell_size, price_sell)
            except:
                #print("sell order failed")
                sell_order = {"id": "failed", "amount":0, "price":last, "side":"sell"}
    else:
        sell_order = {"id": "failed", "amount":0, "price":last, "side":"sell"}
    if buy_size > 0:
        try:
            buy_order = exchange.createOrder(symbol, "limit", "buy", buy_size, price_buy)
        except:
            time.sleep(1)
            try:
                buy_order = exchange.createOrder(symbol, "limit", "buy", buy_size, price_buy)
            except:
                #print("buy order failed")
                buy_order = {"id": "failed", "amount":0, "price":last, "side":"buy"}
    else:
        buy_order = {"id": "failed", "amount":0, "price":last, "side":"buy"}

    return order_list_from_response(sell_order), order_list_from_response(buy_order)

def fetch_trades_eval(exchange, symbol, return_last=False):
    trades = exchange.fetchTrades(symbol)
    trade_prices = []
    for trade in trades:
        trade_prices.append(trade["price"])
    trades_range = [max(trade_prices), min(trade_prices)]
    trades_spread = (max(trade_prices)-min(trade_prices))/min(trade_prices)
    trades_last = trade_prices[-1]
    if return_last:
        return trades_last
    return trades_range, trades_spread, trades_last


def get_order_status(exchange, id):
    try:
        return exchange.fetchOrder(id)["status"]
    except:
        return "failed"

def is_order_closed(exchange, id):
    order = exchange.fetchOrder(id)
    return order["status"] == "closed"

def is_order_canceled(exchange, id):
    order = exchange.fetchOrder(id)
    return order["status"] == "canceled"

def cancel_order_by_id(exchange, id):
    try:
        return exchange.cancel_order(id)
    except:
        return "failed"


def high_low_derivate(candles):
    if isinstance(candles, list):
        candles = np.array(candles)
    highs, lows = get_highs_lows_from_candles(candles)
    highs_h, highs_l = get_high_lows(highs)
    lows_h, lows_l = get_high_lows(lows)
    return highs_h, lows_l

def high_low_2nd_derivate(highs, lows):
    highs_h, highs_l = get_high_lows(highs)
    lows_h, lows_l = get_high_lows(lows)
    return highs_h, lows_l

def get_2nd_derivate(exchange, symbol, timeframe="1m", limit=1000, return_first=False, candles=None, till=None, assert_limit=True):
    if candles is None:
        if till is None:
            candles = np.array(exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit))
        else:
            if isinstance(till, str):
                till = str_to_timestamp(till)
            candles = np.array(loop_fetch_candles(exchange, symbol, timeframe,since=None, till=till, limit=limit))
    elif isinstance(candles, list):
        candles = np.array(candles)
    if len(candles) > limit and assert_limit:
        candles = candles[-limit:]
    highs, lows = high_low_derivate(candles)
    if return_first:
        return highs, lows
    highs, lows = high_low_2nd_derivate(highs, lows)
    return highs, lows

def next_higher_in_arrays(array_1, array_2, key, treshhold=0.03):
    next_1 = next_higher_in_array(array_1, key, treshhold)
    next_2 = next_higher_in_array(array_2, key, treshhold)
    if next_2 is None:
        return next_1
    elif next_1 is None:
        return next_2
    elif next_1 > next_2 and next_2 > key:
        return next_2
    return next_1

def next_lower_in_arrays(array_1, array_2, key, treshhold=0.03):
    next_1 = next_lower_in_array(array_1, key, treshhold)
    next_2 = next_lower_in_array(array_2, key, treshhold)
    if next_2 is None:
        return next_1
    elif next_1 is None:
        return next_2
    elif next_1 < next_2 and next_2 < key:
        return next_2
    return next_1

def next_higher_lower_in_array(array, key):
    if isinstance(array, list) or isinstance(array, pd.Series):
        array = np.array(array)
    higher = next_higher_in_array(array, key, treshhold=0.0)
    lower = next_lower_in_array(array, key, treshhold=0.0)
    return [lower, higher]

def next_higher_in_array(array, key, treshhold=0.0):
    a = np.sort(array)
    key = key*(1+treshhold)
    try:
        return a[(a > key).argmax()]
    except:
        return None

def safe_from_dict(d: dict, key:str, default: any=None):
    if isinstance(key, dict):
        for k1, k2 in key.items():
            if k1 in d and k2 in d[k1]:
                return d[k1][k2]
            else:
                return default
    if key in d:
        return d[key]
    else:
        return default

def next_lower_in_array(array, key, treshhold=0.03):
    a = -np.sort(-array)
    key = key*(1-treshhold)
    try:
        return a[(a < key).argmax()]
    except:
        return None

def price_in_range(price, range):
    return price > range[0] and price < range[1]

def range_mid(range):
    return (range[0] + range[1])/2

def price_in_range_dict(price, range_dict):
    for key, range in range_dict.items():
        if price_in_range(price, range):
            return key, range
    return None, None

def filter_low_volume_market(symbol, exchange=None):
    import numpy as np
    if exchange is None:
        import ccxt
        exchange = ccxt.ftx()
    candles = np.array(exchange.fetchOHLCV(symbol, timeframe="1m", limit=50))
    vol = candles[:,5]
    i = 0
    for v in vol:
        if v == 0:
            i += 1
    if i > 15:
        return False
    return True

def filter_symbols_low_volume(symbols):
    filtered = []
    for symbol in symbols:
        if filter_low_volume_market(symbol):
            filtered.append(symbol)
    return filtered

def pot_outbreak_position(position, open_orders, last_price, direction):
    size = position[0] if position[0] is not None else 0
    price = position[1] if position[1] is not None else 0
    side = position[2] if position[2] is not None else 0
    amount = -size if side == "sell" else size

    pot_size, pot_value = amount, amount*price
    for order in open_orders:
        if direction == "long":
            if order[2] > last_price:
                _amount = -order[1] if order[3] == "sell" else order[1]
                pot_size += _amount
                pot_value += _amount*order[2]
        else:
            if order[2] < last_price:
                _amount = -order[1] if order[3] == "sell" else order[1]
                pot_size += _amount
                pot_value += _amount*order[2]
    pot_price = pot_value/pot_size if pot_size > 0 else 0
    pot_side = "sell" if pot_size<0 else "buy"
    return [abs(pot_size), abs(pot_price), pot_side]

def calc_pnl(size, entry_price, side, price):
    amount = -size if side == "sell" else size
    value_init = entry_price * amount
    value_calc = price * amount
    return value_calc - value_init

def timeframes_enumerated(num):
    d = {-1: '15s', 0: '1m', 1: '5m', 2: '15m', 3: '1h', 4: '4h', 5: '1d', 6: '3d', 7: '1w', 8: '2w', 9: '1M'}
    return d[num]

def enumerate_timeframes(timeframe):
    d = {-1: '15s',0: '1m', 1: '5m', 2: '15m', 3: '1h', 4: '4h', 5: '1d', 6: '3d', 7: '1w', 8: '2w', 9: '1M'}
    return [k for k,v in d.items() if v == timeframe][0]

def shift_timeframe(timeframe, shift):
    d = {-1: '15s',0: '1m', 1: '5m', 2: '15m', 3: '1h', 4: '4h', 5: '1d', 6: '3d', 7: '1w', 8: '2w', 9: '1M'}
    key = [k for k,v in d.items() if v == timeframe][0]
    return d[key + shift]

def next_higher_lower_in_timeframes(exchange, symbol, _price, candles=None, treshhold=0.02, till=None, assert_limit=True):
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
        _highs, _lows = get_2nd_derivate(exchange,symbol, timeframe=timeframes_enumerated(i), limit=1000, candles=candles,till=till, assert_limit=assert_limit)
        _high = next_higher_in_arrays(_highs, _lows, high_ref_price, treshhold=treshhold)
        _low = next_lower_in_arrays(_highs, _lows, low_ref_price, treshhold=treshhold)
        if _high is not None and _high > high_ref_price and high is None:
            high = _high
        if _low is not None and _low < low_ref_price and low is None:
            low = _low
        if high is None or low is None:
            candles = None
        i += 1
    return high, low

def find_markets_by_spread(exchange: object, min_spread: float=0.001, watch_list: list=["USDT/USD:USD"], min_volume: float=2000, spread_type: str="trades"):
    import asyncio
    import Misso.services.async_helper as ah
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # 'RuntimeError: There is no current event loop...'
        loop = None
    if loop is not None and loop.is_running():
        thread = ah.RunThread(ah.get_markets_by_min_spread, [exchange, min_spread, watch_list, min_volume, spread_type])
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.run(ah.get_markets_by_min_spread(exchange, min_spread, watch_list, min_volume, spread_type))

def execute_asyncio(method, args, kwargs):
    import asyncio
    from Misso.services.async_helper import RunThread
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # 'RuntimeError: There is no current event loop...'
        loop = None
    if loop is not None and loop.is_running():
        thread = RunThread(method, args, kwargs)
        thread.start()
        thread.join()
        return thread.result
    else:
        return asyncio.run(method(*args, **kwargs))



def find_outbreak_target(exchange,symbol, range, treshhold=0.02, candles=None, till=None):
    high_target, low_target = next_higher_lower_in_timeframes(exchange, symbol, range,treshhold=treshhold, candles=candles, till=till)
    return high_target, low_target

def get_dca_levels(exchange, symbol, price, direction, num_levels=5, step=0.02, init_treshhold=0.02):
    idx = 1 if direction == "buy" else 0
    dca_levels = [price]
    for i in range(num_levels):
        _treshhold = init_treshhold + (i * step)
        _range = next_higher_lower_in_timeframes(exchange, symbol, price, treshhold=_treshhold)
        if _range[idx] is not None:
            price = _range[idx]
        else:
            price = price * (1-_treshhold) if idx == 1 else price * (1+_treshhold)
        dca_levels.append(price)
    return dca_levels

def is_valid_current_ranges(ranges):
    return all(isinstance(r, dict) for r in ranges)

def filter_failed_gather_ranges(ranges, watchlist):
    valid = []
    for r in ranges:
        if isinstance(r, dict):
            valid.append(list(r.keys())[0])
    out = []
    for symbol in watchlist:
        if not symbol in valid:
            out.append(symbol)
    return out

def get_target_range_array(exchange, symbol, candles):
    t1 = []
    t2 = []
    t3 = []
    _t1 = None
    _t2 = None
    _t3 = None
    for i in range(len(candles)):
        ref_range = [candles[i,2], candles[i,3]]
        till = candles[i,0]
        if _t1 is not None and _t1[0] is not None and _t1[1] is not None:
            if ref_range[0] < _t1[0] and ref_range[1] > _t1[1]:
                t1.append(_t1)
                t2.append(_t2)
                t3.append(_t3)
                continue
        if i < 1000:
            h1, l1 = next_higher_lower_in_timeframes(exchange, symbol, ref_range, candles=None, till=till)
            _t1 = [h1, l1]
            try:
                h2, l2 = next_higher_lower_in_timeframes(exchange, symbol, _t1, treshhold=0.04, candles=None, till=till)
                _t2 = [h2, l2]
            except:
                _t2 = [None, None]
            try:
                h3, l3 = next_higher_lower_in_timeframes(exchange, symbol, _t2, treshhold=0.06, candles=None, till=till)
                _t3 = [h3, l3]
            except:
                _t3 = [None, None]
        else:
            h1, l1 = next_higher_lower_in_timeframes(exchange, symbol, ref_range, candles=candles[:i], till=till, assert_limit=False)
            _t1 = [h1, l1]
            try:
                h2, l2 = next_higher_lower_in_timeframes(exchange, symbol, _t1, treshhold=0.04, candles=candles[:i], till=till, assert_limit=False)
                _t2 = [h2, l2]
            except:
                _t2 = [None, None]
            try:
                h3, l3 = next_higher_lower_in_timeframes(exchange, symbol, _t2, treshhold=0.06, candles=candles[:i], till=till, assert_limit=False)
                _t3 = [h3, l3]
            except:
                _t3 = [None, None]
        t1.append(_t1)
        t2.append(_t2)
        t3.append(_t3)
    return t1, t2, t3

def get_size_from_factor(dtarget, spread, factor=1):
    return dtarget/(spread*factor)

def cancel_order_get_position_delta(exchange, order, position, err_logger=None):
    if order["status"] != "canceled" and not order["status"] == "closed":
        try:
            exchange.cancel_order(order["id"])
        except Exception as e:
            if err_logger is not None:
                err_logger.error(e)
            #print("order seems to be closed get position delta")
    order = exchange.fetch_order(order["id"])
    _value = order["filled"] *  order["price"]
    _side = order["side"]
    _sign = -1 if _side != position[3] else 1
    value = position[0] + _value * _sign
    size = position[1] + order["filled"] * _sign
    price = value/size
    side = position[3] if size > 0 else opposite_side(position[3])
    return [value, size, price, side]


def set_stoploss_for_pot_position(exchange,symbol, pot_position, ref_price, direction):
    pos_size = pot_position[0]
    pos_side = pot_position[2]
    if pos_size > 0:
        side = opposite_side(pos_side)
        if direction == "long":
            if pos_side == "buy":
                return None
            stop_price = ref_price * 1.005
        else:
            if pos_side == "sell":
                return None
            stop_price = ref_price *0.995
        try:
            order = exchange.create_order(symbol, "stop", side, pos_size, params={"triggerPrice":stop_price, "reduceOnly":True})
            return order["id"]
        except:
            return None

def minutes_from_timeframe(timeframe):
    d = {"15s":0.25, "1m":1, "5m":5, "15m": 15, "30m":30, "1h": 60, "4h":240,"12h":720, "1d":1440}
    return d[timeframe]

def get_attribute_dict_from_list(cls, attributes):
    attributes_dict = {}
    for _attribute in attributes:
        attribute = getattr(cls, _attribute)
        if isinstance(attribute, dict):
            for _key, value in attribute.items():
                key = f"{_attribute}_{_key}"
                if isinstance(value, bool):
                    if value:
                        _value = 1
                    else:
                        _value = 0
                else:
                    _value = value
                attributes_dict[key] = _value
        else:
            if _attribute == "is_near_next_resistance" or _attribute == "is_near_next_support":
                if attribute == False:
                    _value = 0
                else:
                    _value = 1
            elif isinstance(attribute, bool):
                if attribute:
                    _value = 1
                else:
                    _value = 0
            else:
                _value = attribute
            attributes_dict[_attribute] = _value
    return attributes_dict

def order_list_to_dict(order):
    return {"id":order[0], "amount":order[1], "price":order[2], "side":order[3]}

def order_dict_to_list(order):
    return [order["id"], order["amount"], order["price"], order["side"], order["symbol"]]

def average_from_orders(orders):
    id = 0
    amount = 0
    value = 0
    for order in orders:
        if order[3] == "buy":
            _amount = order[1]
        else:
            _amount = -order[1]
        value += _amount * order[2]
        amount += _amount
        symbol = order[4]
    side = "buy" if amount >= 0 else "sell"
    return [id, abs(amount), value/amount, side, symbol]

def get_dca_exit_order_from_position(position, order_size):
    if position["is_open"]:
        dca_order = [0, order_size, position["dca_levels"][position["dca_count"]], position["side"]]
        exit_order = [0, position["size"], position["exit_target"], opposite_side(position["side"])]
        return dca_order, exit_order

def update_position_by_closed_order(exchange, position, order):
    last_price = exchange.fetchTicker(position["symbol"])["last"]
    if not position["is_open"]:
        position["size"] = order["amount"]
        position["filled"] = 0
        position["value"] = order["amount"]*order["price"]
        position["avg_price"] = order["price"]
        position["side"] = order["side"]
        position["is_open"] = True
        position["is_pending"] = False
        position["unit_value"] = position["value"]
        position["dca_count"] += 1
        position["dca_levels"] = get_dca_levels(exchange, position["symbol"], last_price, position["side"])
        position["exit_target"] = find_outbreak_target(exchange, position["symbol"], last_price)[side_to_index(position["side"])]
        position["current_exit_size"] = order["amount"]
        position["orders_set"] = [False, False]

    else:
        if position["side"] == order["side"]:
            position["size"] += order["amount"]
            position["value"] += order["amount"]*order["price"]
            position["avg_price"] = position["value"]/position["size"]
            position["dca_count"] += 1
            position["exit_target"] = find_outbreak_target(exchange, position["symbol"], last_price)[side_to_index(position["side"])]
            position["orders_set"] = [False, False]
        else:
            position["size"] -= order["amount"]
            if position["size"] == 0:
                position = reset_position_dict(position["symbol"])
            elif position["size"] < 0:
                _side = opposite_side(position["side"])
                _value = abs(position["value"] - order["amount"]*order["price"])
                _size = abs(position["size"])
                _avg_price = _value/_size
                position = reset_position_dict(position["symbol"])
                position["side"] = _side
                position["dca_count"] = 1
                position["value"] = _value
                position["avg_price"] = _avg_price
                position["is_open"] = True
                position["is_pending"] = False
                position["unit_value"] = _value
                position["dca_levels"] = get_dca_levels(exchange, position["symbol"], last_price, position["side"])
                position["exit_target"] = find_outbreak_target(exchange, position["symbol"], last_price)[side_to_index(position["side"])]
                position["orders_set"] = [False, False]
            else:
                position["value"] -= order["amount"]*order["price"]
                position["avg_price"] = position["value"]/position["size"]
    return position


def position_update_by_line(position, order):
    # placeholder not finised
    position["symbol"] = order["amount"]
    position["size"] = order["amount"]
    position["filled"] = order["amount"]
    position["value"] = order["amount"]
    position["avg_price"] = order["amount"]
    position["side"] = order["amount"]
    position["is_open"] = order["amount"]
    position["is_pending"] = order["amount"]
    position["unit_value"] = order["amount"]
    position["dca_count"] = order["amount"]
    position["dca_levels"] = order["amount"]
    position["opening_range"] = order["amount"]
    position["exit_target"] = order["amount"]
    position["current_exit_size"] = order["amount"]
    return position


def reset_position_dict(symbol):
    return {"symbol":symbol,
            "size":0,
            "filled":0,
            "value":0,
            "avg_price":0,
            "side":None,
            "is_open":False,
            "is_pending":False,
            "unit_value":0,
            "dca_count":0,
            "dca_levels":[],
            "opening_range":[],
            "exit_target":0,
            "current_exit_size":0,
            "orders_set":[False, False]}

def position_update_dca_levels(exchange, position, last_price=None):
    last_price = exchange.fetchTicker(position["symbol"])["last"] if last_price is None else last_price
    position["dca_levels"] = get_dca_levels(exchange, position["symbol"], last_price, position["side"])
    return position

def side_to_index(side, return_exit=True):
    if return_exit:
        return 0 if side == "buy" else 1
    return 1 if side == "buy" else 0

def position_update_exit_target(exchange, position, last_price=None):
    last_price = exchange.fetchTicker(position["symbol"])["last"] if last_price is None else last_price
    current_range = find_outbreak_target(exchange, position["symbol"], last_price)
    position["exit_target"] = current_range[0] if position["side"] == "buy" else current_range[1]
    return position

def position_increase_by_order(position, order):
    position["size"] += order["amount"]
    position["value"] += order["amount"]*order["price"]
    position["avg_price"] = position["value"]/position["size"]
    position["dca_count"] += 1
    return position

def is_uncovered_exit_size(position):
    return position["filled"] + position["size"] < position["current_exit_size"]

def get_order_size(exchange, last_price, market, value):
    min_order_size = exchange.market(market)["precision"]["amount"]
    order_size = value/last_price if value/last_price >= min_order_size else min_order_size
    order_size = float(exchange.amount_to_precision(market, order_size))
    return order_size

def generate_exit_order(position):
    return [0, position["size"]+position["filled"], position["exit_target"], opposite_side(position["side"]),
            position["symbol"]]

def place_limit_market_order(exchange, symbol, size, side, check_size=True):
    if check_size:
        if size < exchange.markets[symbol]["limits"]["amount"]["min"]:
            size = exchange.markets[symbol]["limits"]["amount"]["min"]
    pside = "bid" if side == "sell" else "ask"
    price = exchange.fetch_ticker(symbol)[pside]
    order = [0, size, price, side, symbol]
    order = create_limit_order(exchange, order)
    if order[0] != "failed":
        return order
    else:
        order[1] = 0
        return order

def create_limit_order(exchange, order):
    from ccxt.base.errors import InsufficientFunds
    try:
        resp = exchange.create_limit_order(order[4], order[3], order[1], order[2])
        return [resp["id"], order[1], order[2], order[3], order[4]]
    except InsufficientFunds as err:
        return ["failed", order[1], order[2], "InsufficientFunds", order[4]]
    except Exception as e:
        print(e)
    return ["failed", order[1], order[2], "Error", order[4]]

def create_stop_order(exchange, order):
    from ccxt.base.errors import InsufficientFunds
    try:
        assert isinstance(order[2], list), "order[2] should be type list [price, triggerPrice]"
        resp = exchange.create_order(order[4], "stop", order[3], order[1], order[2][0], params={"stopPrice":order[2][1]})
        return [resp["id"], order[1], order[2], order[3], order[4]]
    except InsufficientFunds as err:
        return ["failed", order[1], order[2], "InsufficientFunds", order[4]]
    except Exception as e:
        print(e)
    return ["failed", order[1], order[2], "Error", order[4]]

def create_conditional_order(exchange: object, symbol: str, amount: float, trigger_price: float, price: float=None):
    try:
        resp = exchange.create_order(symbol, "stop", amount, price, params={"stopPrice": trigger_price})
        return resp
    except Exception as e:
        print(e)

def generate_multi_exit_orders(exchange, position, num, digits=4):
    total_size = position["size"]+position["filled"]
    remain_size = total_size
    if position["side"] == "buy":
        sign = 1
        is_valid_exit = (position["exit_target"] > position["avg_price"]) if position["exit_target"] is not None else False
    else:
        sign = -1
        is_valid_exit = (position["exit_target"] < position["avg_price"]) if position["exit_target"] is not None else False
    if not is_valid_exit or position["exit_target"] is None:
        position["exit_target"] = position_update_exit_target(exchange, position, position["avg_price"])["exit_target"]
    try:
        target_range = abs(position["exit_target"] - position["avg_price"])
    except:
        target_range =abs(position["avg_price"] * (1+sign*0.01) - position["avg_price"])


    orders = []
    for i in range(num):
        if round(total_size/num, digits) > remain_size:
            size = remain_size
        elif i+1 == num:
            size = remain_size
        else:
            size = round(total_size/num, digits)
        if size < exchange.market(position["symbol"])["precision"]["amount"]:
            size = exchange.market(position["symbol"])["precision"]["amount"]
        price = position["avg_price"] + sign * target_range/num * (i+1)
        orders.append([0, size, round(price, digits), opposite_side(position["side"]), position["symbol"]])
        remain_size -=size
        if remain_size <= 0:
            break
    return orders



def generate_dca_order(exchange, position, last_price):
    if position["dca_count"] >= len(position["dca_levels"]):
        return ["failed"]
    if position["side"] == "buy" and last_price < position["dca_levels"][position["dca_count"]]:
        price = last_price
    elif position["side"] == "sell" and last_price > position["dca_levels"][position["dca_count"]]:
        price = last_price
    else:
        price = position["dca_levels"][position["dca_count"]]
    value = position["unit_value"] + position["unit_value"]*(position["dca_count"]-1)
    size = get_order_size(exchange, last_price, position["symbol"], value)
    return [0, size, price, position["side"], position["symbol"]]

def get_next_dca_range(exchange: ccxt, position: object, init_delta=0.01, step=0.01):
    idx = 0 if position.side == "buy" else 1
    last_dca_level = position.current_range[idx]
    next_delta = init_delta + (position.dca_count-1) * step
    next_range = get_dca_levels(exchange, position.symbol, last_dca_level, position.side, 1, init_treshhold=next_delta)
    high = max(next_range)
    low = min(next_range)
    low = last_dca_level * (1-next_delta) if low is None else low
    high = last_dca_level * (1+next_delta) if high is None else high
    return [float(exchange.price_to_precision(position.symbol, low)), float(exchange.price_to_precision(position.symbol, high))]

def force_gernerate_dca_order(exchange, position, last_price, value=None):
    price = last_price
    if value is None:
        value = position["unit_value"] + position["unit_value"]*(position["dca_count"]-1)
    size = get_order_size(exchange, last_price, position["symbol"], value)
    return [0, size, price, position["side"], position["symbol"]]

def open_orders_to_average_buy_sell(orders):
    avg_buys = []
    avg_sells = []
    for order in orders:
        if isinstance(order, dict):
            order = order_list_from_response(order)
        if order[3] == "buy":
            avg_buys.append(order)
        else:
            avg_sells.append(order)
    return average_from_orders(avg_buys), average_from_orders(avg_sells)

def create_limit_order_from_list(exchange, symbol, order, return_order_list=True, add_info=None):
    from ccxt.base.errors import InsufficientFunds
    if return_order_list:
        try:
            resp = exchange.create_limit_order(symbol, order[3], order[1], order[2])
            return [resp["id"], order[1], order[2], order[3], add_info]
        except InsufficientFunds as err:
            return ["failed", order[1], order[2], "InsufficientFunds", add_info]
    try:
        return exchange.create_limit_order(symbol, order[3], order[1], order[2])
    except InsufficientFunds as err:
        return err

def norm_orders_by_distance(avg_buy_order, avg_sell_order, ref_price):
    normed_buy_size = avg_buy_order[1] / abs(ref_price-avg_buy_order[2])
    normed_sell_size = avg_sell_order[1] / abs(ref_price-avg_sell_order[2])
    normed_diff = normed_buy_size - normed_sell_size
    side = "sell" if normed_diff < 0 else "buy"
    if side == "sell":
        return [0, abs(normed_diff)*abs(ref_price-avg_sell_order[2]), avg_sell_order[2], "sell"]
    else:
        return [0, abs(normed_diff)*abs(ref_price-avg_buy_order[2]), avg_buy_order[2], "buy"]


def timeframe_string_to_seconds(timeframe):
    if timeframe.endswith("s"):
        t = int(timeframe[:-1])
    elif timeframe.endswith("m"):
        t = int(timeframe[:-1])*60
    elif timeframe.endswith("s"):
        t = int(timeframe[:-1])
    elif timeframe.endswith("h"):
        t = int(timeframe[:-1])*60*60
    elif timeframe.endswith("d"):
        t = int(timeframe[:-1])*60*60*24
    return t

def timeframe_string_to_unit(timeframe, unit="minutes"):
    if unit == "seconds":
        return timeframe_string_to_seconds(timeframe)
    if unit == "minutes":
        return timeframe_string_to_seconds(timeframe)/60
    if unit == "hours":
        return timeframe_string_to_seconds(timeframe)/60/60
    if unit == "days":
        return timeframe_string_to_seconds(timeframe)/60/60/24

def safe_format_since(since):
    if isinstance(since, str):
        since = str_to_timestamp(since) * 1000
    return since



def loop_fetch_candles(exchange, symbol, timeframe, since, till=None, limit=None, limit_per_request=5000, candles=None):
    """ :param since UST timestamp (int)"""
    import numpy as np
    import time

    if since is not None:
        since = int(since) if since > 900000000000 else int(since*1000)

    interval_s = timeframe_string_to_seconds(timeframe)
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
            _candles = np.array(exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=_start, limit=_limit))
            if candles is None:
                candles = _candles
            else:
                candles = np.append(candles, _candles, axis=0)
        else:
            break
        _start = _end
        _end += max_request_ms
    return candles


def console_flush(m, counter, side_len=30, status_max=10):
    header = f"\n\n{m['proc_name']} - {m['symbol']}      #{m['cycle_num']} Cycle ~{round(m['cycle_duration'],2)}s"
    header_fill = side_len*2 - len(header)
    restricted = ["proc_name", "symbol", "cycle_num", "cycle_duration", "i"]
    header = header + f"{' '*header_fill}\n\n{'_'*side_len*2}"
    table = header
    i = 4
    for key, value in m.items():
        if not key in restricted:
            if value == "subtitle":
                sep = f"\n{'-'*side_len*2}"
                sep_fill = " "*(side_len*2 - len(key) - 6)
                sep = sep + f"\n|    {key}{sep_fill}"
                row = sep + f"\n|{' '* (side_len*2-3)}|"
                i += 3
            else:
                key_len = len(str(key))
                value_len = len(str(value))
                left_fill = " "*(side_len-5-key_len)
                right_fill = " "*(side_len-5-value_len)
                row = f"\n|   {key}{left_fill}|   {value}{right_fill}|"
                i += 1
            table = table + row
    footer = f"\n{'_'*side_len*2}"
    footer = footer + f"\n\nCycle Progress >> {'#'*m['i']}{'.'*(status_max - m['i'])} |"
    lines = i + 5
    return table + footer, lines

def ffill_blank(text, length):
    return f"{text}{' '*(length-len(text))}"

def ffill_blank_end(text, length, end="|"):
    return f"{text}{' '*(length-len(text)-1)}{end}"

def console_flush_multiprocessing(process_dicts, side_len=30, status_max=3):
    rows = [" "]
    processes = list(process_dicts.keys())
    #header
    vert_row = f"{'-'*side_len*(len(process_dicts)+1)}"
    blanc_col = f"{' '*(side_len-1)}|"

    header = ffill_blank("   PROCESS PERFORMANCE OVERVIEW:", side_len)
    for key, m in process_dicts.items():
        _col_header = f"{m['proc_name']} - {m['symbol']}"
        header = header + ffill_blank(_col_header, side_len)
    rows.append(header)

    subheader = ffill_blank_end(" ", side_len)
    for process, m in process_dicts.items():
        if "cycle_duration" in m:
            _col_header = f"   #{m['cycle_num']} Cycle ~{round(m['cycle_duration'],2)}s"
        else:
            _col_header = f"   #{m['i']} "
        subheader = subheader + ffill_blank(_col_header, side_len)
        m_keys = list(m.keys())
    rows.append(subheader)
    rows.append(" ")
    rows.append(vert_row)
    restricted = ["proc_name", "symbol", "cycle_num", "cycle_duration", "i"]
    for key in m_keys:
        # subtitles
        if key.startswith("_"):
            rows.append(vert_row)
            _row = f"|    {key[1:]}"
            row = ffill_blank_end(_row, side_len) + f"{blanc_col*len(processes)}"
            rows.append(row)
        elif not key in restricted:
            _row = f"|   {key}"
            row = ffill_blank_end(_row, side_len)
            for proc in processes:
                m = process_dicts[proc]

                row = row + ffill_blank_end(f"    {str(m[key])}",side_len)
            rows.append(row)
    rows.append(vert_row)

    #footer
    footer = blanc_col
    for proc in processes:
        m = process_dicts[proc]
        _progress = f">> {'#'*m['i']}{'.'*(status_max - m['i'])}"
        footer = footer + ffill_blank_end(_progress, side_len)
    rows.append(footer)
    return rows


def calc_relative_spread(max, min):
    return (max-min)/min

def calc_abs_spread(max, min):
    return max-min

def get_range_trend_shift(_highs):
    highs, _ = get_high_lows(_highs)
    a = np.array(highs)
    _a = np.diff(a)
    _a = np.sign(_a)
    last = 4 if len(_a) > 4 else len(_a)
    if np.sum(_a[-last:]) <= -2:
        return -1
    if np.sum(_a[-last:]) >= 2:
        return 1
    return 0

