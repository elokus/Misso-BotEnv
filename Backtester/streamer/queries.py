import Backtester.time.time_utility as tm
from Backtester.types import QueryCandles, QueryCandlesMeta, QueryCandlesPayload, QueryCandlesRes

def missing_limit(query) -> QueryCandles:
    query["limit"] = int((query["end"] - query["start"]) / tm.tx_in_ms(query["timeframe"]))
    return query

def missing_end(query) -> QueryCandles:
    query["end"] = int(query["start"] + query["limit"] * tm.tx_in_ms(query["timeframe"]))
    return query

def missing_start(query) -> QueryCandles:
    query["start"] = int(query["end"] - query["limit"] * tm.tx_in_ms(query["timeframe"]))
    return query

def missing_start_end(query) -> QueryCandles:
    query["end"] = int(tm.get_timestamp())
    return missing_start(safe_timestamps(query))

def safe_timestamps(query: QueryCandles) -> QueryCandles:
    for key in ["start", "end"]:
        if key in query:
            if isinstance(query[key], str):
                query[key] = tm.to_tms(query[key])
            query[key] = tm.round_timestamp(query[key], query["timeframe"])
    return query

def safe_timestamp(value) -> int:
    return tm.safe_tms(value)

def validate_query(query: QueryCandles) -> QueryCandles:
    match query:
        case {"max": int() as max, "limit": limit} if max < limit:
            raise AttributeError(f"query length {limit} exceeds max {max}")
        case {"start": start, "end":end} if start >= tm.MIN_TIMESTAMP and end <= tm.NOW():
            return query
        case _:
            raise AttributeError("Query params not valid. Check start and end timestamp")

def safe_query(query: dict) -> QueryCandles:
    query = safe_timestamps(query)
    match query:
        case {"start": int(), "end": int(), "limit": int()}:
            return validate_query(query)
        case {"start": int(), "end": int()}:
            return validate_query(missing_limit(query))
        case {"start": int(), "limit": int()}:
            return validate_query(missing_end(query))
        case {"end": int(), "limit": int()}:
            return validate_query(missing_start(query))
        case {"limit": int()}:
            return validate_query(missing_start_end(query))
        case _:
            raise AttributeError("Insufficient query paramater. Please provide start | end | limit")


def safe_queries(queries: list) -> list[QueryCandles]:
    return [safe_query(query) for query in queries]


class QueryBuilder:
    META_KEYS = ["exchange", "market", "timeframe"]
    PAYLOAD_KEYS = ["start", "end", "limit"]

    def build_one(self, exchange: str, market: str, timeframe: str, **kwargs) -> QueryCandles:
        return  safe_query({
            "exchange": exchange,
            "market": market,
            "timeframe": timeframe,
            **kwargs
        })

    def build_many(self, exchange: str, markets: list | str, timeframes: list | str, **kwargs) -> list[QueryCandles]:
        markets = [markets] if isinstance(markets, str) else markets
        timeframes = [timeframes] if isinstance(timeframes, str) else timeframes
        return [self.build_one(exchange, m, t, **kwargs) for t in timeframes for m in markets]

    def edit_one(self, query: QueryCandles, **kwargs) -> QueryCandles:
        return safe_query({"exchange":query["exchange"],
                           "market": query["market"],
                           "timeframe": query["timeframe"],
                           **kwargs
                           })


    def _safe_query(self, **kwargs) -> QueryCandles:
        return safe_query(kwargs)


    #-------------- not in use
    def _split_query(self, query: QueryCandles) -> tuple[QueryCandlesMeta, QueryCandlesPayload]:
        return self._get_meta(**query), self._get_payload(**query)

    def _get_meta(self, **kwargs) -> QueryCandlesMeta:
        return {k: kwargs[k] for k in self.META_KEYS}

    def _get_payload(self, **kwargs) -> QueryCandlesPayload:
        return {k: kwargs[k] for k in self.PAYLOAD_KEYS if k in kwargs}