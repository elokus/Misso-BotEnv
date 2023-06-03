import ccxt, time, asyncio, threading
from dataclasses import dataclass, field
import Misso.services.utilities as utils
import Misso.services.fetch_utilities as fetch
import Misso.services.cust_decorator as deco
import nest_asyncio

class CandleStreamer:
    def __init__(self, watch_list, timeframes=["1m", "15m", "1h", "4h"], **kwargs):
        self.watch_list = watch_list
        self.timeframes = timeframes
        self.save_to_file = True
        self.stop = False
        self.is_new_watch_list = False
        self.new_watch_list = None
        self.logger = None
        self.name = "[CandleStreamer]"
        self.is_stopped=True
        self.init_candles = False
        self.save_on_tx = "15m"
        self.file_path = "storage/candles.json"
        self.limit = 500
        self.init_kwargs(**kwargs)
        self.freq_seconds = {tx:int(utils.timeframe_string_to_seconds(tx)) for tx in self.timeframes}
        self.data = {symbol:{tx:[] for tx in self.timeframes} for symbol in self.watch_list}
        self.last_updates = {tx:int(time.time()) for tx in self.timeframes}
        if utils.is_running_in_jupyter():
            nest_asyncio.apply()
        self.init_all_candles()
        self.log("initialized")


    def init_kwargs(self, **kwargs):
        if len(kwargs) > 0:
            for attr, arg in kwargs.items():
                setattr(self, attr, arg)

    def init_all_candles(self):
        self.update_candles_txs(self.timeframes)
        self.init_timestamps()




    def init_timestamps(self, ref_market="BTC/USD:USD"):
        while not self.init_candles:
            print("waiting for init candles")
        for tx in self.timeframes:
            txs = utils.last_timestamp_candles(self.data[ref_market][tx]) + 60
            self.last_updates[tx] = txs

    def reset_data_dict(self):
        self.data = {symbol:{tx:[] for tx in self.timeframes} for symbol in self.watch_list}

    def change_watch_list(self, new_watch_list):
        self.new_watch_list = new_watch_list
        self.is_new_watch_list = True

    def _change_watch_list(self):
        self.watch_list = self.new_watch_list
        self.is_new_watch_list = False
        self.reset_data_dict()
        self.init_all_candles()



    @deco.run_in_thread_deamon
    def run(self):
        self.init_all_candles()
        self.is_stopped = False
        while not self.stop:
            if self.is_new_watch_list:
                self._change_watch_list()
            self.run_updates()
            time.sleep(60)
        self.is_stopped = True

    def run_updates(self):
        txs = self.get_update_timeframes()
        if len(txs) > 0:
            self.update_candles_txs(txs)
            if self.save_to_file and self.save_on_tx in txs:
                self.save_to_json()

    def get_update_timeframes(self):
        txs = []
        for tx in self.timeframes:
            if self.is_new_timestep(tx):
                txs.append(tx)
        return txs

    def is_new_timestep(self, tx):
        return self.freq_seconds[tx] + self.last_updates[tx] <= int(time.time())

    def update_candles_txs(self, txs: list):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        data = asyncio.run(fetch.get_candles_txs(self.watch_list, txs))
        for market, tx_data in data.items():
            self.data[market].update(tx_data)
        self.update_timestamps(txs)
        self.init_candles = True
        return

    def update_timestamps(self, txs):
        txs = [txs] if not isinstance(txs, list) else txs
        for tx in txs:
            self.last_updates[tx] = int(time.time())

    @property
    def _last_updates(self):
        return {k: utils.timestamp_to_str(v) for k, v in self.last_updates.items()}

    def log(self, msg):
        if self.logger is not None:
            self.logger.info(f"{self.name}: {msg}")

    def save_to_json(self):
        utils.save_to_json_dict(self.data, "storage/candles.json")

    # only for jupyter execution