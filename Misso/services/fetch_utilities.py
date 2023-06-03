import asyncio, ccxt
import ccxt.async_support as accxt
import numpy as np
import Misso.services.utilities as utils


## ASYNC methods

async def get_candles_txs(symbols: list, txs: list=["1m", "15m", "1h", "4h"], limit: int=500, since: float=None, **kwargs):
    exchange = accxt.ftx({'options': {'adjustForTimeDifference': True}})

    since = utils.safe_format_since(since)
    tasks = [_get_candle(exchange, symbol, tx, limit, since, **kwargs) for tx in txs for symbol in symbols]
    data = await asyncio.gather(*tasks, return_exceptions=True)
    candles = {symbol:{tx:None for tx in txs} for symbol in symbols}
    for d in data:
        for k, v in d.items():
            candles[k].update(v)
    await exchange.close()
    return candles

async def _get_candle(exchange: ccxt, symbol: str, tx: str, limit: int, since: int, **kwargs):
    try:
        resp = await exchange.fetchOHLCV(symbol, timeframe=tx, limit=limit, since=since)
    except:
        resp = None
    return {symbol:{tx:np.array(resp)}}


async def get_ohlcv_data_multi_timeframe(a_exchange, watch_list, limit=500, close=False, since=None, timeframes=["1m", "15m", "1h", "4h"]):
    if a_exchange is None:
        a_exchange = accxt.ftx()
    if isinstance(watch_list, str):
        watch_list = [watch_list]
    symbol_timeframes = [(s, t) for s in watch_list for t in timeframes]
    if since is not None:
        since = utils.safe_format_since(since)
    tasks = [_get_ohlcv_data_multi_timeframe(a_exchange, sym, tx, limit, since=since) for sym, tx in symbol_timeframes]
    data = await asyncio.gather(*tasks, return_exceptions=True)

    candles = {}
    failed = False
    for d in data:
        if not isinstance(d, dict):
            print(d)
            failed = True
            continue
        for k, v in d.items():
            if not k in candles:
                candles[k] = {}
            candles[k][v["timeframe"]] = v["data"]
    if failed:
        print(data)
    if close:
        await a_exchange.close()
    return candles

def match_closed_stop_orders(exchange, since=None):
    stop_orders = exchange.fetch_orders(since=None, params={"type":"stop"})
    closed_stop = {}
    for o in stop_orders:
        if o["status"] == "closed":
            cso = {"symbol":o["symbol"], "price": float(o["info"]["orderPrice"]), "amount":o["amount"], "side":o["side"], "timestamp":o['lastTradeTimestamp']}
            cso = _match_order_id(exchange, cso)
            closed_stop[o["id"]] = cso
    return closed_stop


def _match_order_id(exchange, cso: dict):
    orders = exchange.fetch_orders(cso["symbol"], since=cso["timestamp"], limit=10)
    for o in orders:
        if o["timestamp"] - 10000 < cso["timestamp"] and o["price"] == cso["price"] and o["side"] == cso["side"] and o["amount"] == cso["amount"]:
            return o["id"]
    return None


async def _get_ohlcv_data_multi_timeframe(a_exchange: accxt, symbol: str, timeframe: list, limit: int=500, since: int=None):
    resp = await a_exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit, since=since)
    return {symbol:{"timeframe":timeframe, "data":np.array(resp)}}

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
        return asyncio.new_event_loop().run_until_complete(method(*args, **kwargs))


# symbols = ["BTC", "ETH", "CEL"]
# res = asyncio.run(get_candles_txs(symbols))
# print("RESULTS:")
# print(res)