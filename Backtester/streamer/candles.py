# Combines database and API call in single interface
from __future__ import annotations
from abc import ABC, abstractmethod
import numpy as np
import ccxt.async_support as ccxt
import Backtester.utilities as utils
from Backtester.database.candles import CandlesDb
from Backtester.types import QueryCandles, QueryCandlesMeta, QueryCandlesPayload, QueryCandlesRes
from Backtester.streamer.queries import safe_query, safe_timestamps, safe_timestamp, QueryBuilder
from Backtester.streamer.fetch import fetch_query_api, fetch_queries_api

import asyncio, nest_asyncio, sys



class CandlesAPI:
    def get(self, query: QueryCandles) -> QueryCandlesRes:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        return asyncio.run(fetch_query_api(query))

    def gather(self, queries: list[QueryCandles]) -> list[QueryCandlesRes]:
        if len(queries) == 0:
            return []
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        return asyncio.run(fetch_queries_api(queries))



class CandlesInterface:

    query = QueryBuilder()
    api = CandlesAPI()
    db = CandlesDb()
    presets = {}

    def __init__(self):
        if "ipykernel_launcher.py" in sys.argv[0]:
            nest_asyncio.apply()

    def gather(self, exchange: str, markets: list | str, timeframes: list | str, **kwargs) -> list[QueryCandlesRes]:
        queries = self.query.build_many(exchange, markets, timeframes, **kwargs)
        return self._gather(queries)


    def get(self, exchange: str, market: str, timeframe: str, **kwargs) -> QueryCandlesRes:
        query = self.query.build_one(exchange, market, timeframe, **kwargs)
        print(query)
        return self._get(query)

    def get_kwargs(self, **kwargs) -> QueryCandlesRes:
        return self._get(self.query._safe_query(**kwargs))

    def _get(self, query: QueryCandles) -> QueryCandlesRes:
        gaps = self.db.has_not(query)
        while len(gaps) > 0:
            self._fetch_store_missing(gaps)
            gaps = self.db.has_not(query)
        return self.db.get(query)

    def _gather(self, queries: list[QueryCandles]) -> list[QueryCandlesRes]:
        gaps = self.db.have_not(queries)
        while len(gaps) > 0:
            self._fetch_store_missing(gaps)
            gaps = self.db.have_not(queries)
        return self.db.gather(queries)

    def _fetch_store_missing(self, gaps: list[tuple]) -> None:
        _queries = [self.query.edit_one(q, start=s, end=e) for q, (s, e) in gaps]
        print("fetching gaps from api:")
        print(_queries)
        res = self.api.gather(_queries)
        self.db.store_queries(res)
        print("gaps stored to db")

    def get_price_at(self, exchange, market, timeframe, timestamp):
        from Backtester.time.time_utility import offset_tms
        price = self._get_price_at(exchange, market, timeframe, timestamp)
        if not price:
            self.get(exchange, market, timeframe, start=offset_tms(timestamp, timeframe, -1), limit=5)
            return self._get_price_at(exchange, market, timeframe, timestamp)
        return price

    def _get_price_at(self, exchange, market, timeframe, timestamp):
        return self.db.fetch_price({
            "exchange": exchange,
            "market": market,
            "timeframe": timeframe},
            safe_timestamp(timestamp))


    # def _get(self, query: QueryCandles) -> QueryCandlesRes:
    #     if self.db.has(query):
    #         print(f"get from db: {self.query._get_meta(**query)}")
    #         return self.db.get(query)
    #     return self._fetch_store_one(query)

    # def _gather(self, queries: list[QueryCandles]) -> list[QueryCandlesRes]:
    #     db_queries, api_queries = self.db.have(queries)
    #     for query in db_queries:
    #         print(f"gather from db: {self.query._get_meta(**query)}")
    #     res = self.db.gather(db_queries)
    #     res += self._fetch_store_many(api_queries)
    #     return res
    #
    # def _fetch_store_one(self, query: QueryCandles) -> QueryCandlesRes:
    #     res = self.api.get(query)
    #     self.db.store_query(*res)
    #     return res
    #
    # def _fetch_store_many(self, queries: list[QueryCandles]) -> list[QueryCandlesRes]:
    #     res = self.api.gather(queries)
    #     self.db.store_queries(res)
    #     return res

streamer = CandlesInterface()
# class CandlesModeObject(ABC):
#
#     @abstractmethod
#     def prepare_query(self, *args, **kwargs):
#         pass
#
#
# class CandlesLive(CandlesModeObject):
#     pass
#
#
# class CandlesBacktest(CandlesModeObject):
#     pass
#
#
# class CandlesStreamer:
#     """Frontend Interface"""
#     def __init__(self):
#         self._instance = self._get_instance(utils.get_env_mode())
#
#     def _get_instance(self, MODE: str) -> CandlesModeObject:
#         match MODE:
#             case "live" | "livetest":
#                 return CandlesLive()
#             case "backtest" | "simulation":
#                 return CandlesBacktest()
#             case _:
#                 raise ValueError