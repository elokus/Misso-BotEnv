import asyncio
import importlib
import Misso.services.helper as ph
import Misso.services.async_helper as ah
#from Misso.services.orders import Order


class BaseMethods:

    ###############################################
    ###   Dynamic Method Creation:
    ###
    def get_input(self, input_attributes):
        print("[get_input] input_attributes:", input_attributes)
        if input_attributes is None:
            return
        if not isinstance(input_attributes, list):
            input_attributes = [input_attributes]
        inputs = []
        for input in input_attributes:
            if isinstance(input, str):
                if input.startswith("self."):
                    inputs.append(getattr(self, input.split(".")[1]))
                else:
                    inputs.append(input)
            else:
                inputs.append(input)
        return inputs

    async def async_get_set(self, config):
        modul = importlib.import_module(config["modul"])
        method = getattr(modul, config["method"])
        value = await method(*self.get_input(config["input_args"]))
        if not config["target_save_update"]:
            setattr(self, config["target_attr"], value)
        else:
            self.save_update(config["target_attr"], value)
        if config["return_value"]:
            return value

    def save_update(self, target_attr, value):
        if isinstance(getattr(self, target_attr), dict):
            for key, value in value.items():
                getattr(self, target_attr)[key] = value

    def get_set(self, config):
        modul = importlib.import_module(config["modul"])
        method = getattr(modul, config["method"])
        value = method(*self.get_input(config["input_args"]))
        if not config["target_save_update"]:
            setattr(self, config["target_attr"], value)
        else:
            self.save_update(config["target_attr"], value)
        if config["return_value"]:
            return value
    ###############################################
    ###   Main Treading TASKS:
    ###

    def process_finished_orders(self):
        name = "process_finished_orders"
        unprocessed_orders = ph.find_processed_orders(self.open_orders, is_processed=False)
        finished_unprocessed_orders = ph.find_orders_by_not_status(unprocessed_orders, not_status="open")
        for order in finished_unprocessed_orders:
            if order.status == "closed":
                self.on_closed_order(order)
            elif order.status == "canceled":
                self.on_canceled_order(order)  #updating position in case of parially filled orders
            else:
                print(f"UNKNOWN order.status={order.status}")
            order.is_processed = True

    def process_open_orders(self):
        """check open positions for partially filled orders to keep position accurat. Used every N runs"""

    def on_closed_order(self, order):
        self.update_position_by_order(order)

    def on_canceled_order(self, order):
        if order.filled > 0.0 and order.filled != order._filled:
            self.update_position_by_order(order)
            order._filled = order.filled


        # unfinished_orders = ph.find_unfinished_orders(self.open_orders)
        # closed_orders = ph.find_orders_by_status(unfinished_orders, "closed")
        # canceled_orders = ph.find_orders_by_status(unfinished_orders, "canceled")
        # failed_orders = ph.find_orders_by_status(unfinished_orders, "failed")
        # not_open_orders = ph.find_orders_not_open(unfinished_orders)

    ###############################################
    ###   Async Subthreading TASKS:
    ###

    async def t_init_update(self, task_config):
        name = task_config["name"]
        for subtask, config in task_config["subtasks"].items():
            if config["type"] == "async":
                resp = await self.async_get_set(config)
            else:
                resp = self.get_set(config)
        self.first_update = True

    async def t_update_tickers(self, task_config):
        name = task_config["name"]
        while not self.stop_updates:
            try:
                while self.restart_exchange:
                    await asyncio.sleep(5)
                tickers = await self.aftx.fetch_tickers(self.watch_list)
                for symbol, ticker in tickers.items():
                    self.last_price[symbol] = ticker["last"]
                self.thread_logger.info(f"t_Modul {name} finished")
                self.system_status[name] = True
                await asyncio.sleep(task_config["freq"])
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
                continue
            if self.stop_updates:
                break
        self.stop_updates = True
        await asyncio.sleep(1)
        return

    async def t_system_status(self, task_config):
        freq = task_config["freq"]
        await asyncio.sleep(100)
        while not self.stop_updates:
            msg = f" Free Risk Capital {self.free_risk_margin} | Free Priority Capital {self.free_risk_margin_priority} | Total Capital: {self.total_capital}, #Open Units {self.open_units_counter}, total unit risk {self.total_unit_risk}, next_unit_value {self.next_unit_value}/max.:{self.unit_value_factors[0]*self.total_capital}"
            ph.telegram_notify_msg(msg, self.error_logger)
            if len(self.system_status) > 0:
                for key in self.system_status.keys():
                    self.system_status[key] = None
                await asyncio.sleep(task_config["freq"])
                ph.evaluate_system_status(self.system_status.copy(), self.error_logger)
            if self.stop_updates:
                break
            await asyncio.sleep(freq)

    async def t_shutdown(self, task_config):
        while True:
            if self.stop_updates:
                break
            await asyncio.sleep(5)
        asyncio.set_event_loop(self.loop)
        self.thread_logger.info(f"Restarting update tasks ... Canceling tasks")
        self.error_logger.error("shutting down update tasks")
        tasks = [t for t in asyncio.all_tasks(self.loop) if t is not asyncio.current_task()]
        [task.cancel() for task in tasks]

        self.thread_logger.info(f"Canceling {len(tasks)} tasks")
        await asyncio.gather(*tasks, return_exceptions=True)
        self.loop.stop()

    async def t_update_open_orders_dict(self, task_config):
        freq = task_config["freq"]
        name = task_config["name"]
        while not self.stop_updates:
            try:
                while self.restart_exchange:
                    await asyncio.sleep(1)
                open_orders = self.open_orders.copy()
                if len(open_orders) > 0:
                    for market, orders in open_orders.items():
                        if len(orders) > 0:
                            for order in orders:
                                if isinstance(order, list) or order["status"] == "open":
                                    self.open_orders[market].remove(order)
                                    id = order[0] if isinstance(order, list) else order["id"]
                                    while self.restart_exchange:
                                        await asyncio.sleep(5)
                                    order = await ah.get_order_by_id(self.aftx, id)
                                    self.open_orders[market].append(order)
                self.thread_logger.info(f"t_Modul {name} finished")
                self.system_status[name] = True
                await asyncio.sleep(freq)
            except Exception as e:
                self.error_logger.info(f"ERROR {name} wit Error: {e}", exc_info=True)
                self.system_status[name] = False
        self.stop_updates = True
        await asyncio.sleep(1)

    async def t_update_open_orders(self, task_config):
        """update status and remove orders in self.open_orders"""
        name = task_config["name"]
        freq = task_config["freq"]
        print(f"starting {name}")
        while not self.stop_updates:
            open_orders = ph.find_orders_by_status(self.open_orders, "open")
            for order in open_orders:
                if not ph.order_in_is_open_orders(order, self.is_open_orders):
                    while self.restart_exchange:
                        await asyncio.sleep(2)
                    response = await ah.get_order_by_id(self.aftx, order.id)
                    order.parse_update(response)
            #remove processed orders (closed/canceled/failed and processed)
            for order in ph.find_processed_orders(self.open_orders):
                self.open_orders[order.symbol].remove(order)
            self.system_status[name] = True
            if self.stop_updates:
                break
            await asyncio.sleep(freq)
        await asyncio.sleep(0.1)

    async def t_execute_orders(self, task_config): #v1
        freq = task_config["freq"]
        name = task_config["name"]
        while not self.stop_updates:
            try:
                while not self.pending_orders.empty():
                    order = self.pending_orders.get()
                    symbol = order[4]
                    while self.restart_exchange:
                        await asyncio.sleep(1)
                    order = await ah.create_limit_order_from_list(self.aftx, symbol, order)
                    if order[0] != "failed":
                        self.open_orders.append(Order(*order))
                    else:
                        self.thread_logger.info(f"[t_execute_orders] failed with order {order}")
                    await asyncio.sleep(1)
                    self.thread_logger.info(f"t_Modul {name} executed order {order}")
                self.thread_logger.info(f"t_Modul {name} finished")
                self.system_status[name] = True
                await asyncio.sleep(freq)
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
        self.stop_updates = True
        await asyncio.sleep(0.1)

    async def t_update_balance(self, task_config):
        freq = task_config["freq"]
        name = task_config["name"]
        while not self.stop_updates:
            try:
                while self.restart_exchange:
                    await asyncio.sleep(5)
                try:
                    balance = await self.aftx.fetch_balance()
                except:
                    try:
                        await asyncio.sleep(10)
                        while self.restart_exchange:
                            await asyncio.sleep(5)
                        balance = await self.aftx.fetch_balance()
                    except:
                        self.error_logger.error("ERROR update_balance failed restarting thread")
                        self.thread_logger.info("ERROR update_balance failed restarting thread")
                        self.system_status[name] = True
                        continue
                self.total_capital = balance["total"]["USD"]
                self.free_capital = balance["free"]["USD"]
                if self.initial_capital == 0:
                    self.initial_capital = self.total_capital
                self.system_status[name] = True
                await asyncio.sleep(freq)
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
                if self.stop_updates:
                    break
                await asyncio.sleep(freq)
        self.stop_updates = True
        await asyncio.sleep(1)

    async def t_restart_exchange(self, task_config):
        name = task_config["name"]
        freq = task_config["freq"]
        await asyncio.sleep(freq)
        while not self.stop_updates:
            try:
                self.restart_exchange = True
                await asyncio.sleep(10)
                await self.aftx.close()
                self.aftx = ph.initialize_exchange_driver(self.subaccount, init_async=True)
                self.restart_exchange = False
                self.thread_logger.info(f"t_Modul restart exchange finished")
                await asyncio.sleep(freq)
                self.system_status[name] = True
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
            i = 0
            while i < freq:
                await asyncio.sleep(5)
                i += 5
                if self.stop_updates:
                    break
        return

    async def t_get_open_orders(self, task_config):
        name = task_config["name"]
        freq = task_config["freq"]
        print(f"starting {name}")
        while not self.stop_updates:
            try:
                while self.restart_exchange:
                    await asyncio.sleep(2)
                response = await self.aftx.fetch_open_orders()
                is_open_orders = ph.parse_is_open_orders(response)
                with self.lock_thread:
                    self.is_open_orders = is_open_orders
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
            if self.stop_updates:
                break
            await asyncio.sleep(freq)
        await asyncio.sleep(0.1)

    async def t_get_open_positions(self, task_config):
        name = task_config["name"]
        freq = task_config["freq"]
        print(f"starting {name}")
        while not self.stop_updates:
            try:
                while self.restart_exchange:
                    await asyncio.sleep(2)
                response = await ah.get_open_positions(self.aftx)
                with self.lock_thread:
                    self.is_open_positions = response
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
            if self.stop_updates:
                break
            await asyncio.sleep(freq)
        await asyncio.sleep(0.1)

    async def t_update_template(self, task_config):
        name = task_config["name"]
        print(f"starting update template {name}")
        while not self.first_update:
            await asyncio.sleep(5)
        while not self.stop_updates:
            try:
                for subtask, config in task_config["subtasks"].items():
                    while self.restart_exchange:
                        await asyncio.sleep(2)
                    if config["type"] == "async":
                        resp = await self.async_get_set(config)
                    else:
                        resp = self.get_set(config)
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
            if self.stop_updates:
                break
            await asyncio.sleep[task_config["freq"]]
        await asyncio.sleep(0.1)

    async def t_update_ticker_ws(self, task_config):
        name = task_config["name"]
        print(f"starting update {name}")
        while not self.first_update:
            await asyncio.sleep(5)
        while not self.stop_updates:
            try:
                self.update_ticker()
                self.update_positions()
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
            if self.stop_updates:
                break
            await asyncio.sleep(task_config["freq"])
        await asyncio.sleep(0.1)

    async def t_update_candles(self, task_config):
        name = task_config["name"]
        print(f"starting update {name}")
        while not self.first_update:
            await asyncio.sleep(5)
        while not self.stop_updates:
            try:
                candles = await ah.get_ohlcv_data(*self.get_input(task_config["input_args"]))
                with self.lock_thread:
                    self.save_update(task_config["out"], candles)
            except Exception as e:
                print(f"ERROR restarting [{name}]", e)
                self.error_logger.error(e, exc_info=True)
                self.system_status[name] = False
            if self.stop_updates:
                break
            await asyncio.sleep(task_config["freq"])
        await asyncio.sleep(0.1)