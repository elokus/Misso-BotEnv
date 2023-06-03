from Backtester.streamer.candles import streamer
from Backtester.indicators.wave_cluster import get_wave_levels
import Backtester.time.time_utility as timly


class SimpleStreamer:
    """ Preset parameter and methods for faster streamer access"""
    def __init__(self, exchange: str, symbol: str, tx: str = "1m", txs: list = None):
        self.exchange = exchange
        self.symbol = symbol
        self.tx = tx   #main timeframe
        self.txs = txs if txs else ["1m", "15m", "1h", "4h"]
        self._last_tms = streamer.db.fetch_last(self._query_meta)
        self._last_price = self._price_at(self._last_tms)

    def build_query(self, **kwargs) -> dict:
        return streamer.query.build_one(*self._meta, **kwargs)

    def build_queries(self, **kwargs) -> list[dict]:
        return streamer.query.build_many(*self._metas, **kwargs)

    def get(self, **kwargs) -> tuple:
        return streamer.get(*self._meta, **kwargs)

    def gather(self, **kwargs) -> list[tuple]:
        return streamer.gather(*self._metas, **kwargs)

    def get_levels(self, at: str | int) -> (float, list):
        l = self._levels_at(at)
        p = self._price_at(at)
        return p, l

    def gen_query(self, start: str | int, end: str | int = None):
        """ Meta Query from start till end of dataset or test range"""
        end = self._last_tms if not end else end
        _q = {"start": timly.safe_tms(start), "end": timly.safe_tms(end)}
        return {**self._query_meta, **_q}

    @property
    def _query_meta(self):
        return {"exchange": self.exchange, "market": self.symbol, "timeframe": self.tx}

    @property
    def _meta(self):
        return self.exchange, self.symbol, self.tx

    @property
    def _metas(self):
        return self.exchange, self.symbol, self.txs

    def _offset_tms(self, tms: int | str, offset: int) -> int:
        _tms = timly.safe_tms(tms)
        return int(_tms + offset * timly.tx_in_ms(self.tx))

    def _prev_tms(self, tms):
        return self._offset_tms(tms, -1)

    def _levels_at(self, at: int | str) -> dict:
        return get_wave_levels(streamer.gather(*self._metas, end=self._prev_tms(at), limit=500))

    def _price_at(self, at: int | str) -> float:
        return streamer.get_price_at(*self._meta, at)

    def _indicator_at(self, at: int | str) -> float:
        pass

    def _date_iterator(self, start, end, freq="2D", astype="str"):
        import pandas as pd
        dr = pd.date_range(start, end, freq=freq)
        if astype == "str":
            return [str(d) for d in dr]
        if astype == "ms" or astype == "int":
            return [d.value // 10**6 for d in dr]
        return list(dr)


