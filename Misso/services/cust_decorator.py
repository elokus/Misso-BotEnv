import queue
import asyncio
import threading
import time
import ccxt

from Misso.services.logger import error_logger

# def wait_queue(func):
#     def wrapper_wait_queue(q: queue.Queue(), *args, **kwargs):
#         print("starting wait_queue inner")
#         while not q.empty():
#             print("loop inner")
#             msg = q.get()
#             if msg is None:
#                 break
#             print(f"before function call. Message: {msg}")
#             func(msg, *args, **kwargs)
#         print("after function call stopping loop")
#
#     return wrapper_wait_queue

def with_semaphore(func):
    def task_wrapper(self, *args, **kwargs):
        with self.semaphore:
            try:
                func(self, *args, **kwargs)
            except Exception as e:
                if hasattr(self, "name"):
                    error_logger.error(f"ERROR in {self.name}: {e}", exc_info=True)
                self.logger.error(f"ERROR in with semaphore: {e}", exc_info=True)
        time.sleep(5)
    return task_wrapper


def run_in_thread_if_failed(func):
    def task_wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except:
            thread = threading.Thread(target=func, args=(*args, ), kwargs=(kwargs))
            thread.start()
            thread.join()
    return task_wrapper


def run_in_thread_deamon(func):
    def task_wrapper(*args, **kwargs):
        thread = threading.Thread(target=func, args=(*args, ), kwargs=(kwargs))
        thread.deamon = True
        thread.start()
    return task_wrapper

def run_in_thread(func):
    def task_wrapper(*args, **kwargs):
        thread = threading.Thread(target=func, args=(*args, ), kwargs=(kwargs))
        thread.start()
    return task_wrapper

def safe_request(func):
    def task_wrapper(*args, **kwargs):
        try:
            response = func(*args, **kwargs)
            return response
        except (ccxt.NetworkError, ccxt.RequestTimeout, ccxt.ExchangeNotAvailable) as e:
            time.sleep(20)
            error_logger.error(f"SAFE_REQUEST ERROR: {e}  .... restarting exchange")
            args = _unified_exchange_restart(*args)
            try:
                response = func(*args, **kwargs)
                return response
            except Exception as e:
                error_logger.error(f"SAFE_REQUEST ERROR: {e}  ")
                return "failed"
        except ccxt.InsufficientFunds as e:
            error_logger.error(f"SAFE_REQUEST ERROR: {e}  returning Balance Warning")
            return "InsufficientFunds"
        except Exception as e:
            error_logger.error(f"SAFE_REQUEST ERROR: {e}  ")
            return "failed"
    return task_wrapper


def async_update_wrapper(func):
    async def task_wrapper(self, *args, **kwargs):
        i = 0
        while not self.stop:
            try:
                while self.restart_exchange:
                    await asyncio.sleep(5)
                resp = await func(i, *args, **kwargs)
                for key, value in resp.items():
                    getattr(self, kwargs["target_attr"])[key] = value
                await asyncio.sleep(kwargs["freq"])
                i += 1
                if i > 100:
                    break
            except Exception as e:
                self.error_logger.info(f"{kwargs['name']} ERROR: {e}", exc_info=True)
        self.stop = True
        await asyncio.sleep(0.1)
    return task_wrapper

### UTILITYS
def _unified_exchange_restart(*args):
    if isinstance(args[0], ccxt.ftx):
        exchange = _restart_exchange(args[0])
        _args = [exchange]
        if len(args) > 1:
            for arg in args[1:]:
                _args.append(arg)
            args = tuple(_args)
    elif hasattr(args[0], "_exchange"):
        exchange = _restart_exchange(getattr(args[0], "_exchange"))
        setattr(args[0], "_exchange", exchange)
    elif hasattr(args[0], "exchange"):
        exchange = _restart_exchange(getattr(args[0], "exchange"))
        setattr(args[0], "exchange", exchange)
    return args

def _restart_exchange(exchange):
    from Misso.services.helper import initialize_exchange_driver, change_sub
    set_sub = False
    if "FTX-SUBACCOUNT" in exchange.headers:
        subaccount = exchange.headers["FTX-SUBACCOUNT"]
        set_sub = True
    exchange = initialize_exchange_driver("Main")
    if set_sub:
        exchange = change_sub(exchange, subaccount)
    return exchange