from __future__ import annotations
import uuid
import numpy as np
from peewee import fn
from Backtester.database.models.candle import Candle
from Backtester.database.objects import DatabaseInterface
from Backtester.database.helper import get_gaps
from Backtester.types import QueryCandles, QueryCandlesRes, dbGaps





class CandlesDb(DatabaseInterface):
    db = Candle

    def has_not(self, query: QueryCandles) -> dbGaps:
        return get_gaps(self.db, query)

    def have_not(self, queries: list[QueryCandles]):
        gaps = []
        for q in queries:
            gaps += self.has_not(q)
        return gaps

    def has(self, query: QueryCandles) -> bool:
        _length = self.db.select().where(
            self.db.exchange == query["exchange"],
            self.db.symbol == query["market"],
            self.db.timeframe == query["timeframe"],
            self.db.timestamp.between(query["start"], query["end"])).count()
        return _length >= int(query["limit"] * 0.95)

    def have(self, queries: list[QueryCandles]) -> tuple[list[QueryCandles], list[QueryCandles]]:
        found, not_found = [], []
        for query in queries:
            if self.has(query):
                found.append(query)
            else:
                not_found.append(query)
        return found, not_found

    def store_query(self, query: QueryCandles, candles: np.ndarray | list) -> None:
        if np.ndim(candles) == 1:
            self._store(self._entry(query["exchange"], query["market"], query["timeframe"], candles))
        elif np.ndim(candles) == 2:
            self._store(self._entries(query["exchange"], query["market"], query["timeframe"], candles))

    def store_queries(self, result_list: list[QueryCandlesRes]) -> None:
        for result in result_list:
            self.store_query(*result)

    def get(self, query: QueryCandles) -> QueryCandlesRes:
        res = (self.db.select(
            self.db.timestamp, self.db.open, self.db.high, self.db.low, self.db.close, self.db.volume
            ).where(
                self.db.exchange == query["exchange"],
                self.db.symbol == query["market"],
                self.db.timeframe == query["timeframe"],
                self.db.timestamp.between(query["start"], query["end"])
            ).order_by(self.db.timestamp.asc()).tuples())
        return query, self._to_list(res)

    def gather(self, queries: list[QueryCandles]) -> list[QueryCandlesRes]:
        if len(queries) == 0:
            return []
        return [self.get(query) for query in queries]


    def fetch(self, exchange: str, market: str, timeframe: str, start: int, finish: int) -> np.ndarray:
        return np.array(tuple(
            self.db.select(
                self.db.timestamp, self.db.open, self.db.high, self.db.low, self.db.close,
                self.db.volume
            ).where(
                self.db.exchange == exchange,
                self.db.symbol == market,
                self.db.timeframe == timeframe,
                self.db.timestamp.between(start, finish)
            ).order_by(self.db.timestamp.asc()).tuples()
        ))

    def fetch_x(self, timeframe):
        res = tuple(self.db
                    .select(self.db.symbol, fn.MAX(self.db.timestamp), fn.MIN(self.db.timestamp))
                    .where(self.db.timeframe == timeframe)
                    .group_by(self.db.symbol)
                    .order_by(self.db.symbol).tuples()
                    )
        return res

    def fetch_last(self, meta) -> int:
        return self.db.select(fn.MAX(self.db.timestamp)).where(
            self.db.exchange == meta["exchange"],
            self.db.symbol == meta["market"],
            self.db.timeframe == meta["timeframe"]).scalar()


    def fetch_price(self, meta: dict, tms: int) -> float:
        return self.db.select(self.db.open).where(
            self.db.exchange == meta["exchange"],
            self.db.symbol == meta["market"],
            self.db.timeframe == meta["timeframe"],
            self.db.timestamp == tms).scalar()

    def _entry(self, exchange: str, symbol: str, timeframe: str,  candle: np.ndarray | list) -> dict:
        return {
            'id': uuid.uuid4(),
            'exchange': exchange,
            'symbol': symbol,
            'timeframe': timeframe,
            'timestamp': candle[0],
            'open': candle[1],
            'high': candle[2],
            'low': candle[3],
            'close': candle[4],
            'volume': candle[5]
        }

    def _entries(self, exchange: str, symbol: str, timeframe: str,  candles: np.ndarray | list) -> list:
        ls = []
        for candle in candles:
            ls.append(self._entry(exchange, symbol, timeframe, candle))
        return ls

    def _to_list(self, data) -> list[list]:
        return [list(e) for e in data]

    def _store(self, data: dict | list) -> None:
        if isinstance(data, dict):
            self.db.insert(**data).on_conflict_ignore().execute()
        if isinstance(data, list):
            self.db.insert_many(data).on_conflict_ignore().execute()


    # def store(self, data: dict | list, on_conflict: str = "ignore") -> None:
    #     if on_conflict == 'ignore':
    #         self.db.insert(**data).on_conflict_ignore().execute()
    #     elif on_conflict == 'replace':
    #         self.db.insert(**data).on_conflict(
    #             conflict_target=['exchange', 'symbol', 'timeframe', 'timestamp'],
    #             preserve=(self.db.open, self.db.high, self.db.low, self.db.close, self.db.volume),
    #         ).execute()
    #     elif on_conflict == 'error':
    #         self.db.insert(**data).execute()
    #     else:
    #         raise Exception(f'Unknown on_conflict value: {on_conflict}')
