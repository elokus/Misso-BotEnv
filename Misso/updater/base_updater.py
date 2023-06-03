from Misso.updater.base_methods import BaseMethods
from Misso.updater.websocket_client import FtxWebsocketClient
import threading
import asyncio
import queue
import Misso.services.helper as ph
import Misso.services.logger as lg
import time



class BaseUpdater(BaseMethods):
    def __init__(self, config_path, subaccount=None):
        super().__init__()
        self.updater_config = ph.parse_config_yaml(config_path)
        self.subaccount = subaccount

        for var, attr in self.updater_config["class_attributes"].items():
            setattr(self, var, attr)

        self.ftx = ph.initialize_exchange_driver(self.subaccount)
        self.ftx.load_markets()
        self.aftx = ph.initialize_exchange_driver(self.subaccount, init_async=True)
        self.client = None
        self.lock_thread = threading.Lock()
        self.stop_thread = threading.Event()
        self.pending_orders = queue.Queue()
        self.logger = None
        self.error_logger = lg.error_logger
        self.thread_logger = lg._init_thread_logger("asyncUpdate", log_to_console=True)

        #states
        self.first_update = False
        self.stop_updates = False
        self.restart_exchange = False

    def run_websocket(self, watch_list):
        self.client = FtxWebsocketClient(self.subaccount)
        self.client.get_orders()
        self.client.get_fills()
        self.client.connect()
        for symbol in watch_list:
            self.client.get_ticker(symbol)

    def run_in_thread(self):
        t_updates = threading.Thread(target=self.loop_in_thread, args=())
        t_updates.start()
        while not self.first_update:
            time.sleep(1)

    def loop_in_thread(self):
        while True:
            self.loop = asyncio.new_event_loop()
            self.aftx = ph.initialize_exchange_driver(self.subaccount, init_async=True)
            self.create_and_run_update_tasks()
            print("restarting create_and_run_update_tasks()")

    def create_and_run_update_tasks(self):
        asyncio.set_event_loop(self.loop)
        for task, config in self.updater_config["tasks"].items():
            self.loop.create_task(getattr(self, task)(config))
        try:
            self.loop.run_forever()

        finally:
            try:
                tasks = asyncio.all_tasks(self.loop)
                for t in [t for t in tasks if not (t.done() or t.cancelled())]:
                    self.loop.run_until_complete(t)
            finally:
                self.log("[create and run update tasks] finally closing loop")
                self.loop.close()