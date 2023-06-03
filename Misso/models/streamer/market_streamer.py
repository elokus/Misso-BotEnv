from Misso.models.streamer.candles import CandleStreamer
from Misso.models.streamer.reviewer import marketReviewer
import Misso.services.utilities as utils
from Misso.services.logger import error_logger
import Misso.services.cust_decorator as deco
import time

class MarketStreamer:
    def __init__(self, **kwargs):
        self.run_id = "00"
        from_file = kwargs["init_from_file"] if "init_from_file" in kwargs else False
        self.watch_list = utils.get_watch_list(from_file=from_file)
        self.timeframes = ["1m", "15m", "1h", "4h"]
        self.storage_dir = "storage"
        self.open_markets = None
        self.logger = None
        self.stop = False
        self.error_logger = error_logger
        self.freq_seconds = 600
        self.restricted = ["USDT/USD:USD", "BNB/USD:USD"]

        self.init_kwargs(**kwargs)
        self.init_watch_list()
        self.name = f"[MarketStreamer_{self.run_id}]"
        self.candles = CandleStreamer(self.watch_list, self.timeframes, logger=self.logger)
        self.clean_watch_list()
        self.reviews = self.get_reviews()
        self.log("initialized")

    def init_kwargs(self, **kwargs):
        if len(kwargs) > 0:
            for attr, arg in kwargs.items():
                setattr(self, attr, arg)

    def init_watch_list(self):
        for restrict in self.restricted:
            if restrict in self.watch_list:
                self.watch_list.remove(restrict)

        if self.open_markets is None:
            return
        for market in self.open_markets:
            if market not in self.watch_list:
                self.watch_list.append(market)

    def clean_watch_list(self, min_tx="1h"):
        changed = False
        for market, tx_data in self.candles.data.items():
            if tx_data[min_tx] is None or len(tx_data[min_tx])< 0.8*self.candles.limit:
                self.watch_list.remove(market)
                changed = True

        market_review = self.get_reviews()
        for market, states in market_review.items():
            if min_tx not in states:
                if market in self.watch_list:
                    self.watch_list.remove(market)
                    self.restricted.append(market)
                    changed = True

        if changed:
            self.candles.change_watch_list(self.watch_list)

    def run(self):
        self.candles.run()
        self.run_market_streamer()

    @deco.run_in_thread_deamon
    def run_market_streamer(self):
        while not self.stop:
            try:
                self.update_reviews()
                self.wait()
                self.save_review()
                self.log(f"updated market review next in {self.freq_seconds}s")

            except Exception as e:
                self.error_logger.error(f"{self.name} ERROR in run_market_streamer", e, exc_info=True)

    def wait(self):
        i = 0
        while i < self.freq_seconds:
            if self.stop:
                break
            time.sleep(1)

    def save_review(self):
        utils.save_to_json_dict(self.reviews, file=f"market_review-{self.run_id}.json", dir=f"{self.storage_dir}\\market")

    def reset_watch_list(self, watch_list):
        self.candles.stop = True
        while not self.candles.is_stopped:
            time.sleep(1)
        self.watch_list = watch_list
        self.candles.watch_list = watch_list
        self.candles.init_all_candles()
        self.candles.run()

    def get_reviews(self, **kwargs):
        return marketReviewer.get_states(self.candles.data, **kwargs)

    def get_review(self, market, **kwargs):
        return marketReviewer.get_state(market, self.candles.data, **kwargs)

    def get_ranked(self, target_tx, **kwargs):
        buys, sells, both = marketReviewer.get_filtered_rr_ranking(self.reviews, target_tx=target_tx, **kwargs)
        return list(buys.keys()), list(sells.keys()), list(both.keys())

    def update_reviews(self):
        self.reviews = self.get_reviews()

    def log(self, msg):
        if self.logger is not None:
            self.logger.info(f"{self.name}: {msg}")