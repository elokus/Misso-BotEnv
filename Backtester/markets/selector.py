import ccxt
from typing import Union
from Risso.exchange.services import exchanges
from Risso.objects.base_objects import MultiDictFilter

class MarketSelector(MultiDictFilter):
    """Load and filter markets aka trading pairs from brokers"""
    def __init__(self):
        super().__init__()

        self.broker_id = "bybit"
        self.exchange: ccxt = exchanges.get(self.broker_id)
        self._tickers: dict = self.exchange.fetch_tickers()
        self._data: dict = self._match(self.exchange.load_markets(), self._tickers)
        self._data_backup: dict = self._data.copy()
        self._add_datasource(self._tickers, "tickers")
        self._add_limits()
        self._add_search_keys(**{"volume":"quoteVolume",
                              "size": "safe_min_size",
                              "value": "safe_min_value"})

    def get_filter_keys(self):
        pass

    def list(self):
        return list(self._data.keys())

    def reset(self):
        self._data = self._data_backup.copy()
        self._history = []

    def filter_markets(self, markets: list):
        params = ("markets", f"in #{len(markets)} list", "match")
        data = {k: v for k, v in self._data.items() if k in markets}
        self._set_filter(data, params)

    def get_tuple_list(self, key: str):
        return [(market, self._safe_lookup(market, key)) for market in self._data.keys()]

    def get_tuples_list(self, *args):
        return [(market, *[self._safe_lookup(market, arg) for arg in args]) for market in self._data.keys()]

    def _match(self, d1: dict, d2: Union[dict, list]) -> dict:
        return {k: v for k, v in d1.items() if k in d2}

    def _safe_size(self, market: str, size: float) -> float:
        return size / self._data[market]["contractSize"]

    def _get_min_size(self, market: str):
        if "safe_min_size" not in self._data[market]:
            self._add_min_size(market)
        return self._data[market]["safe_min_size"]

    def _add_limits(self):
        for m in self._data.keys():
            self._add_min_value(m)
        self._safe_value = True

    def _add_min_value(self, market: str):
        if "safe_min_value" not in self._data[market]:
            self._data[market]["safe_min_value"] = self._get_min_size(market) * self._tickers[market]["last"]

    def _add_min_size(self, market: str):
        if "safe_min_size" not in self._data[market]:
            self._data[market]["safe_min_size"] = self._safe_size(market, self._data[market]["limits"]["amount"]["min"])

    def _get_min_value(self, market: str):
        if "safe_min_value" not in self._data[market]:
            self._add_min_value(market)
        return self._data[market]["safe_min_value"]

    def __repr__(self):
        return f"MarketSelector(Exchange={self.broker_id} #markets={len(self._data)}, active_filters={self._history})"

    def __str__(self):
        filters = "\n".join(self._log)
        return f"MarketSelector(Exchange={self.broker_id} #markets={len(self._data)})   with active filteres: \n{filters}"