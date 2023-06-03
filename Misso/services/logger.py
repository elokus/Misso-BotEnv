import logging
import os

def _init_main_logger(route, session):
    dir = f'storage/logs/{route}/{session}'
    make_directory(dir)
    route_logger = {}
    log_types = ["order", "trade", "position", "executor", "strategy"]
    for log in log_types:
        filename = f'{dir}/{log}.txt'
        new_logger = logging.getLogger(f"{route}-{log}")
        new_logger.setLevel(logging.INFO)
        format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.FileHandler(filename, mode='w')
        handler.setFormatter(format)
        new_logger.addHandler(handler)
        route_logger[log] = new_logger
    return route_logger

def _init_thread_logger(process, dir="storage", log_to_console = False):
    dir = f'{dir}/logs'
    make_directory(dir)
    filename = f'{dir}/{process}_logger.log'
    new_logger = logging.getLogger(f"t_SubManager")
    new_logger.setLevel(logging.INFO)
    #new_logger.setLevel(logging.ERROR)

    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(filename, mode='w')
    handler.setFormatter(format)
    new_logger.addHandler(handler)
    if log_to_console:
        console_handler = logging.StreamHandler()
        new_logger.addHandler(console_handler)
    return new_logger

def _init_process_logger(process):
    dir = f'storage/logs/{process}'
    make_directory(dir)
    filename = f'{dir}/logfile.txt'
    new_logger = logging.getLogger(f"log_{process}")
    new_logger.setLevel(logging.INFO)
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(filename, mode='w')
    handler.setFormatter(format)
    new_logger.addHandler(handler)
    new_logger.propagate = False
    return new_logger

def _init_error_logger():
    dir = f'../storage/logs/errors'
    make_directory(dir)

    filename = f'{dir}/error.txt'
    new_logger = logging.getLogger("errHandler")
    new_logger.setLevel(logging.ERROR)
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.FileHandler(filename, mode='w')
    handler.setFormatter(format)
    new_logger.addHandler(handler)
    return new_logger

def make_directory(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path)

error_logger = _init_error_logger()
#thread_logger = _init_thread_logger("default")