from Backtester.database.models.candle import Candle
from Backtester.types import QueryCandles, QueryCandlesRes, dbGaps


def _get_bounds_sorted(db: object, query: QueryCandles) -> dict[tuple, tuple]:
    from Backtester.time.time_utility import tx_in_ms

    di = tx_in_ms(query["timeframe"])


    db_s = db.alias()
    ntms = db_s.timestamp + di
    ptms = db_s.timestamp - di
    subq = (db_s.select(db_s.timestamp.alias("tms"), ntms.alias("ntms"), ptms.alias("ptms")).where(
        db_s.exchange == query["exchange"],
        db_s.symbol == query["market"],
        db_s.timeframe == query["timeframe"],
        db_s.timestamp.between(query["start"]+2*di, query["end"]-di)))

    topq = db.select(db.timestamp).where(db.exchange == query["exchange"], db.symbol == query["market"], db.timeframe == query["timeframe"])
    query_next = db.select(subq.c.tms).from_(subq).where(subq.c.ntms.not_in(topq))
    query_prev = db.select(subq.c.tms).from_(subq).where(subq.c.ptms.not_in(topq))
    _next = [("n", i[0]) for i in list(query_next.tuples())]
    _prev = [("p",i[0]) for i in list(query_prev.tuples())]
    ls = _next + _prev + [("s", query["start"]), ("e", query["end"])]
    res = sorted(ls, key= lambda x: x[1])
    return {(e[0], res[i+1][0]):(e[1], res[i+1][1]) for i, e in enumerate(res[:-1])}

def get_gaps(db: object, query: QueryCandles) -> dbGaps:
    bounds = _get_bounds_sorted(db, query)
    if ("s", "e") in bounds:
        if _get_size(db, query) == 0:
            return [(query, v) for k, v in bounds.items()]
    return [(query, v) for k, v in bounds.items() if k in [("s", "p"), ("n", "e"), ("n", "p")]]


def _get_size(_db: object, query: QueryCandles) -> int:
    return _db.select().where(
        _db.exchange == query["exchange"],
        _db.symbol == query["market"],
        _db.timeframe == query["timeframe"],
        _db.timestamp.between(query["start"], query["end"])).count()

def query_order_pair(query: dict, bet: tuple, db = Candle, dtms: int = 60000) -> tuple[int, int]:
    from peewee import fn
    dbAlias = db.alias()

    if bet[0] < bet[1]:
        entry_tms = dbAlias.select(fn.MIN(dbAlias.timestamp) + dtms).where(
            dbAlias.exchange == query["exchange"],
            dbAlias.symbol == query["market"],
            dbAlias.timeframe == query["timeframe"],
            dbAlias.timestamp.between(query["start"], query["end"]),
            dbAlias.low < bet[0])
        exit_tms = (db.select(fn.MIN(db.timestamp)).where(db.high > bet[1], db.timestamp.between(entry_tms,  query["end"])))
    else:
        entry_tms = dbAlias.select(fn.MIN(dbAlias.timestamp) + dtms).where(
            dbAlias.exchange == query["exchange"],
            dbAlias.symbol == query["market"],
            dbAlias.timeframe == query["timeframe"],
            dbAlias.timestamp.between(query["start"], query["end"]),
            dbAlias.high > bet[0])
        exit_tms = (db.select(fn.MIN(db.timestamp)).where(db.low < bet[1], db.timestamp.between(entry_tms,  query["end"])))
    _entry_tms = entry_tms.scalar() - dtms if entry_tms.scalar() else None
    return _entry_tms, exit_tms.scalar()
#
# def _tmpl_match_bounds(key) -> str:
#     """ template for all cases bounds"""
#     match key:
#         case ("s", "p"):
#             return "fetch"
#         case ("n", "e"):
#             return "fetch"
#         case ("p", "n"):
#             return "fetch"
#         case ("p", "n"):
#             return "get"
#         case ("p", "e"):
#             return "get"
#         case ("s", "n"):
#             return "get"
#         case ("s", "e"):
#             return "get"

def query_drawdown(meta: dict, db=None, direction="buy"):
    from Backtester.database.models.candle import Candle
    from peewee import Window, fn
    db = Candle if not db else db


    if direction == "buy":
        minq = db.select(fn.MIN(db.low)).where(db.exchange == meta["exchange"],
                                               db.symbol == meta["market"],
                                               db.timeframe == meta["timeframe"],
                                               db.timestamp.between(meta["start"], meta["end"]))

        query = db.select(db.timestamp, db.low).where(db.exchange == meta["exchange"],
                                                      db.symbol == meta["market"],
                                                      db.timeframe == meta["timeframe"],
                                                      db.timestamp.between(meta["start"], meta["end"]),
                                                      db.low == minq)

    else:
        maxq = db.select(fn.MAX(db.high)).where(db.exchange == meta["exchange"],
                                                db.symbol == meta["market"],
                                                db.timeframe == meta["timeframe"],
                                                db.timestamp.between(meta["start"], meta["end"]))
        query = db.select(db.timestamp, db.high).where(db.exchange == meta["exchange"],
                                                       db.symbol == meta["market"],
                                                       db.timeframe == meta["timeframe"],
                                                       db.timestamp.between(meta["start"], meta["end"]),
                                                       db.high == maxq)
    return query

def query_by_tms(meta: dict, timestamps: list, db=None, columns: list=None):
    from Backtester.database.models.candle import Candle
    db = Candle if not db else db
    columns = ["open", "high", "low", "close"] if columns is None else columns
    cols = tuple([getattr(db, col) for col in columns])
    query = db.select(db.timestamp, *cols).where(db.exchange == meta["exchange"],
                                                 db.symbol == meta["market"],
                                                 db.timeframe == meta["timeframe"],
                                                 db.timestamp.in_(timestamps)).order_by(db.timestamp)

    return query.tuples()