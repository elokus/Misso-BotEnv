from typing import TypedDict

class QueryCandlesMeta(TypedDict):
    exchange: str
    market: str
    timeframe: str

class QueryCandlesPayload(TypedDict):
    start: int
    end: int
    limit: int

class QueryCandles(TypedDict):
    exchange: str
    market: str
    timeframe: str
    start: int
    end: int
    limit: int

QueryCandlesRes = tuple[QueryCandles, any]

dbGaps = list[tuple[QueryCandles, tuple]]