import asyncio, time
import ccxt.async_support as ccxt
import Backtester.time.time_utility as tm
from Backtester.types import QueryCandles, QueryCandlesRes


async def fetch_query_api(query: QueryCandles, exchange: ccxt=None) -> QueryCandlesRes:
    """TODO: validate query timestamps espaccially end.
    Also validate returned list"""

    exc = getattr(ccxt, query["exchange"])() if exchange is None else exchange

    data = []
    start = query["start"]

    while start < query["end"] and len(data) < query["limit"]:
        try:
            # _limit = query["limit"] - len(data)+2
            ohlcv = await exc.fetch_ohlcv(query["market"], query["timeframe"], since=start)
            _data = ohlcv[1:(query["limit"] - len(data)+1)]
            if len(_data) == 0:
                break
            data += _data
            print(query, len(data))
            start = data[-1][0]
        except ccxt.BadSymbol:
            return query, "BadSymbol"
        except (ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout) as error:
            print(error)
            time.sleep(10)

    if exchange is None:
        await exc.close()

    return query, data



async def fetch_queries_api(queries: list[QueryCandles], exchange: ccxt=None) -> list[QueryCandlesRes]:
    """Only Used for identical exchange queries"""
    exc = getattr(ccxt, queries[0]["exchange"])() if exchange is None else exchange

    tasks = [fetch_query_api(query, exc) for query in queries]
    data = await asyncio.gather(*tasks, return_exceptions=True)

    if exchange is None:
        await exc.close()

    return data



# non asyncio
#
def _fetch_query(query: QueryCandles, exchange: ccxt=None) -> QueryCandlesRes:
    import ccxt as cxt
    exc = getattr(cxt, query["exchange"])() if exchange is None else exchange

    data = []
    start = query["start"]

    while start < query["end"] and len(data) < query["limit"]:
        try:
            ohlcv = exc.fetch_ohlcv(query["market"], query["timeframe"], start)
            data += ohlcv[1:]
            start = data[-1][0]
        except cxt.BadSymbol:
            return query, "BadSymbol"
        except (cxt.ExchangeError, cxt.AuthenticationError, cxt.ExchangeNotAvailable, cxt.RequestTimeout) as error:
            print(error)
            time.sleep(10)

    return query, data

