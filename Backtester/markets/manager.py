from Backtester.markets.selector import MarketSelector
from Backtester.streamer.candles import CandlesInterface
from Backtester.time.env_time import TimeModul

#   Interface for MarketData
#    Filter Markets, set Environment, update Indicator, stream Candles
#    store Tickers,
#
#



class MarketManager:
    """ Single Exchange Market Manager"""
    markets = MarketSelector()
    candles = CandlesInterface()
    time = TimeModul()
    tickers = None

    def __init__(self):
        self.MODE = "live"
        self.EXCHANGE = "bybit"
        self.TIMEFRAMES = ["1m", "15m", "1h"]
        self.exchange = self.markets.exchange


    @property
    def current_timestamp(self):
        if self.MODE == "live":
            return self.exchange.milliseconds()
        return 0

    def filter(self, **kwargs):
        self.markets.filter(**kwargs)

    def load_all_candles(self, limit=500):
        return self.candles.gather(self.EXCHANGE, self.markets.list(), self.TIMEFRAMES, limit=limit)




if __name__ == "__main__":
    mm = MarketManager()
    mm.filter(type="swap", inverse=False, volume=(5000000,"gt"))