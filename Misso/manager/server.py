import threading

import Misso.services.helper as mh
import Misso.services.cust_decorator as decor
from Misso.manager.wave_client import WaveClient
from Misso.manager.watcher import Watcher
from Misso.models.streamer.market_streamer import MarketStreamer


import logging, queue, os, time, datetime, yaml, json
from threading import Semaphore
from typing import Union
from dataclasses import asdict





###########################################################
###                                                     ###
###         Wrapper for running parallel Strategies     ###
###                                                     ###
###########################################################



class Server:
    def __init__(self, config: Union[dict,str]=None, **kwargs):
        self.name = str(__class__.__name__)
        self.run_id = datetime.datetime.now().strftime("%y%m%d_%H%M")

        self.exchange = mh.initialize_exchange_driver("Main")
        self.exchange.cancel_all_orders()

        self.subaccounts = []
        self.sub_client_configs = {}
        self.restricted = ["BNB/USD:USD", "USDT/USD:USD"]
        self.semaphore = threading.Semaphore(1)

        # PARAMETER
        self.overwrite_logs = True
        self.log_to_console = True
        self.restore_sub_client= True
        self.restore_market_review = False
        self.stop_global = False

        self.init_configs(config)
        self.init_kwargs(**kwargs)
        self.assert_required_attributes()

        self.init_storage_path()
        self.init_messaging_system()
        self.init_logger_system()
        self.init_exchange()
        self.init_market_streamer()

        ## MANAGER
        self.init_watcher()
        self.init_sub_client()

        self.logger.info(f"{self.name} initiated")

    def init_configs(self, config):
        if isinstance(path:=config, str):
            with open(path) as f:
                config = yaml.load(f, Loader=yaml.FullLoader) if path.endswith(".yaml") else json.load(f)

        assert isinstance(config, dict), "TYPE ERROR loading configs assert type: Dict"

        for key, value in config.items():
            setattr(self, key, value)

        if hasattr(self, "strategy_manager_config"):
            for attr, arg in self.strategy_manager_config.items():
                setattr(self, attr, arg)
        print(self.subaccounts)

    def init_kwargs(self, **kwargs):
        if len(kwargs) > 0:
            for attr, arg in kwargs.items():
                setattr(self, attr, arg)

    def assert_required_attributes(self):
        assert hasattr(self, "api_config"), "api_config not defined in config"
        assert hasattr(self, "sub_client_configs"), "sub_client_config not defined in config"
        assert hasattr(self, "storage_dir"), "please define 'storage_dir' in configs"
        assert len(self.subaccounts) > 0, "you should define at least one subaccount in config"
        for sub in self.subaccounts:
            if hasattr(self, "sub_client_configs"):
                if sub not in self.sub_client_configs:
                    assert "default" in self.sub_client_configs, f"sub_client_config not defined for {sub} and no default setting defined"
                    self.sub_client_configs[sub] = self.sub_client_configs["default"]
        if isinstance(self.restricted, list):
            self.restricted = set(self.restricted)

    def init_storage_path(self, main="Misso"):
        p = os.getcwd()
        i = ""
        while os.path.split(p)[1] != main:
            i += "..\\"
            p = os.path.split(p)[0]
        self.storage_dir = f"{i}{self.storage_dir}"

    def init_messaging_system(self):
        self.sub_queues = {sub: queue.Queue() for sub in self.subaccounts}
        self.watcher_queue = queue.Queue()
        self.msg_queue = queue.Queue()

    def init_logger_system(self):
        if not os.path.exists(path := f'{self.storage_dir}/logs'):
            os.makedirs(path)
        filename = f"{path}/{self.name}.log" if self.overwrite_logs else f"{path}/{self.name}_{self.run_id}.log"
        self.logger = logging.getLogger(f"{self.name}_{self.run_id}")
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(filename, mode='w')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(handler)
        log_to_console = self.log_to_console if hasattr(self, "log_to_console") else False
        if log_to_console:
            self.logger.addHandler(logging.StreamHandler())

    def init_exchange(self):
        pass


    def init_market_streamer(self):
        open_markets = self._get_open_markets()
        self.market_streamer = MarketStreamer(run_id=self.run_id, logger=self.logger, open_markets=open_markets)
        self.watch_list = self.market_streamer.watch_list
        self.market_streamer.run()

    def init_watcher(self):
        self.watcher = Watcher(self.watcher_queue, self.msg_queue, subaccounts=self.subaccounts, logger=self.logger)
        self.watcher.run()

    def init_sub_client(self):
        self.sub_client = {}
        for sub in self.subaccounts:
            kwargs = {
                "_exchange": mh.change_sub(self.exchange, sub),
                "config": self.sub_client_configs[sub],
                "out_msg": self.msg_queue,
                "in_msg": self.sub_queues[sub],
                "storage_dir": self.storage_dir,
                "restore_manager": self.restore_sub_client,
                "market_streamer": self.market_streamer,
                "restricted": self.market_streamer.restricted,
                "semaphore": self.semaphore,
                "logger": self.logger,
                "run_id": self.run_id
            }
            self.sub_client[sub] = WaveClient(sub, **kwargs)

    ### ----------------------------- ###

    def run(self, loop=True):
        self.run_messaging()
        self.start_clients()
        # if loop:
        #     self.run_loop()
        # else:
        #     self.run_step(1)

    def run_loop(self):
        try:
            c = 1
            while not self.stop_global:
                c = self.run_step(c)
        finally:
            self.logger.info(f"{self.name} ******* closing loop and saving states ******")
            self.save_states()

    def run_step(self, c):
        self.logger.info(f"{self.name} running next step")
        self.run_clients()
        i = 0
        while i < 60:
            if self.stop_global:
                break
            time.sleep(1)
            i += 1
        return c+1

    def run_clients(self):
        for sub, client in self.sub_client.items():
            client.run_client()

    def start_clients(self):
        for client in self.sub_client.values():
            client.start()


    ### MESSAGE HANDLING ---
    @decor.run_in_thread_deamon
    def run_messaging(self):
        tx = time.time()
        mtx = 20 * 60
        while not self.stop_global:
            try:
                if tx + mtx <= time.time():
                    #self.put_all_subs(("all", "CLEAN"))
                    self.save_states()
                    tx = time.time()
                self.message_processing()
                time.sleep(2)
            except:
                continue

    def message_processing(self):
        while not self.msg_queue.empty():
            msg = self.msg_queue.get()
            queue = self.get_queue(msg)
            queue.put(msg[1])
            self.logger.info(f"{self.name} new message for {msg[0]}")



    def get_queue(self, msg):
        if msg[0] == "watcher":
            return self.watcher_queue
        elif msg[0] == "sub_client":
            return self.sub_queues[msg[1][0]]

    def put_all_subs(self, msg):
        for sub in self.subaccounts:
            self.sub_queues[sub].put(msg)

    ### --- MESSAGE HANDLING



    def save_states(self):
        def factory(data):
            return dict(x for x in data if x[1] is not None)
        for sub, manager in self.sub_client.items():
            sub_states = {}
            for market, pos in manager.pos.items():
                sub_states[market] = asdict(pos, dict_factory=factory)
            mh.save_to_json_dict(sub_states, file=f"{sub}_states_{self.run_id}.json", dir=f"{self.storage_dir}\\restore")

    def get_market_review(self):
        self.market_review = self.market_streamer.reviews
        market_review = self.market_review.copy()
        for market, states in market_review.items():
            if "1h" not in states:
                self.restricted.add(market)
                if market in self.watch_list:
                    self.watch_list.remove(market)
                if market in self.market_review:
                    self.market_review.pop(market)
        mh.save_to_json_dict(self.market_review, file=f"market_review-{self.run_id}.json", dir=f"{self.storage_dir}\\market")

    def get_latest_json(self, prefix: str, dir: str):
        files = [f for f in os.listdir(dir) if f.startswith(prefix)]
        files.sort()
        with open(f"{dir}/{files[-1]}", "r") as f:
            data = json.load(f)
        return data

    def run_safe_guard(self, **kwargs):
        return

    def _get_open_markets(self):
        _wl = []
        for sub in self.subaccounts:
            self.exchange = mh.change_sub(self.exchange, sub)
            opos = self.exchange.fetch_positions()
            open_symbols = [pos["symbol"] for pos in opos if pos["contracts"] > 0]
            _wl.extend(open_symbols)
        return _wl










if __name__ == '__main__':

    config = "../config/strategy_manager.yaml"
    runner = Server(config)
    runner.run()
    try:
        while True:
            time.sleep(1)
    finally:
        print("shutting down submanager")
        runner.stop_global = True





