from __future__ import annotations
from abc import ABC, abstractmethod
import time, os
from Risso.exchange.services import exchanges
from dotenv import load_dotenv

class TimeModul:

    def __init__(self):
        load_dotenv()
        self.MODE = os.getenv("MODE", "live")
        self.EXCHANGE = os.getenv("EXCHANGE", "bybit")
        self.exchange = exchanges.get(self.EXCHANGE)

    def tx_to_s(self, timeframe: str) -> int | None:
        if timeframe.endswith("s"):
            return int(timeframe[:-1])
        elif timeframe.endswith("m"):
            return int(timeframe[:-1])*60
        elif timeframe.endswith("s"):
            return int(timeframe[:-1])
        elif timeframe.endswith("h"):
            return int(timeframe[:-1])*60*60
        elif timeframe.endswith("d"):
            return int(timeframe[:-1])*60*60*24
        return None

    def tx_to_ms(self, timeframe: str) -> int | None:
        return self.tx_to_s(timeframe) * 1000


    @property
    def ref_timestamp(self):
        if self.MODE == "live":
            return self.exchange.milliseconds()
        return int(time.time()*1000)

    def last_timestamp(self, timeframe: str, ref_timestamp: int = None) -> int:
        _ref = ref_timestamp if ref_timestamp else self.ref_timestamp
        return int((_ref // self.tx_to_ms(timeframe)) * self.tx_to_ms(timeframe))

    def last_timestamps(self, timeframes: list, ref_timestamp: int = None) -> dict[str, int]:
        _ref = ref_timestamp if ref_timestamp else self.ref_timestamp
        return {tx: self.last_timestamp(tx, _ref) for tx in timeframes}




class EnvTime:
    """Time Modul for unify time access in backtest simulation and for different exchanges.
    Configuration is loaded from .env file or ENV Variables"""
    def __init__(self):
        pass

    def get_exchange_time(self):
        pass

    def get_system_time(self):
        pass

    def get_simulated_time(self):
        pass


