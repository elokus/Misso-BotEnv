def query_vwap(meta ,db=None):
    from peewee import fn
    from Backtester.database.models.candle import Candle
    db = Candle if not db else db

    _db = db.alias()

    vw_p = (_db.high + _db.low)/2 * _db.volume
    vwp_sum = _db.select(fn.SUM(vw_p).alias("vwp_sum")).where(_db.exchange == meta["exchange"],
                                                              _db.symbol == meta["market"],
                                                              _db.timeframe == meta["timeframe"],
                                                              _db.timestamp.between(meta["start"], meta["end"])).scalar()

    v_sum = db.select(fn.SUM(db.volume)).where(db.exchange == meta["exchange"],
                                               db.symbol == meta["market"],
                                               db.timeframe == meta["timeframe"],
                                               db.timestamp.between(meta["start"], meta["end"])).scalar()
    return vwp_sum / v_sum


def query_vwap_bands(meta, db=None):
    from peewee import fn
    from Backtester.database.models.candle import Candle
    db = Candle if not db else db

    _db = db.alias()

    vw_p = (_db.high + _db.low)/2 * _db.volume
    vwp_sum = _db.select(fn.SUM(vw_p).alias("vwp_sum")).where(_db.exchange == meta["exchange"],
                                                              _db.symbol == meta["market"],
                                                              _db.timeframe == meta["timeframe"],
                                                              _db.timestamp.between(meta["start"], meta["end"])).scalar()

    v_sum = db.select(fn.SUM(db.volume)).where(db.exchange == meta["exchange"],
                                               db.symbol == meta["market"],
                                               db.timeframe == meta["timeframe"],
                                               db.timestamp.between(meta["start"], meta["end"])).scalar()

