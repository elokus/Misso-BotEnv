from dataclasses import dataclass, field
from itertools import count
from datetime import datetime
import numpy as np
import ccxt
import uuid
import Misso.services.helper as mh
from Misso.models import CustomId

counter = count()

@dataclass
class Order:
    size: float
    price: float
    side: str
    symbol: str
    params: dict = field(default_factory=dict)
    id: str = None
    client_id: str = None
    status: str = "generated"
    filled: float = 0.0
    _filled: float = 0.0
    trigger_price: float = None
    remaining: float = None
    value: float = None
    opened_at: datetime = field(default_factory = datetime.utcnow)
    index: int = field(default_factory=lambda: next(counter))
    is_conditional = False
    closed_at: int = None
    is_placed: bool = False
    is_open: bool = False
    is_closed: bool = False
    is_canceled: bool = False

    is_processed: bool = False
    is_partially_filled: bool = False
    is_updated: bool = False


    def __post_init__(self):
        self.remaining = self.size
        self.value = self.size * self.price
        self.trigger_price = self.params["triggerPrice"] if "triggerPrice" in self.params else None
        if "clientId" in self.params:
            self.client_id = self.params["clientId"]
        elif "clientOrderId" in self.params:
            self.client_id = self.params["clientOrderId"]
        else:
            self.client_id = None

    def new_client_id(self, new_id: str=None, sub: str=None):
        if new_id is None:
            sub = self.client_id.split("_")[0] or sub
            self.client_id = CustomId.generate(sub, self.symbol, type="entry", side=self.side)
            if "clientId" in self.params:
                self.params["clientId"] = self.client_id
            if "clientId" in self.params:
                self.params["clientId"] = self.client_id
            elif "clientOrderId" in self.params:
                self.params["clientOrderId"] = self.client_id


    def set_order_opening(self, response: dict=None):
        if isinstance(response, dict):
            self.id = response["id"]
            self.status = "new"
            self.is_placed = True
            self.is_open = True
        if response is None or response == "failed":
            self.id = "failed"
            self.status = "failed"


    def create(self, exchange: ccxt, logger: object=None):
        is_valid = self.safe_format(exchange)
        sub = exchange.headers["FTX-SUBACCOUNT"]
        if not is_valid:
            self.status = "failed"
            print("not valid size")
            return False

        type = "limit" if "triggerPrice" not in self.params else "stop"
        try:
            response = exchange.create_order(self.symbol, type, self.side, self.size, self.price, params=self.params)
            self.set_order_opening(response)
            if logger is None:
                print(f"{self.symbol} placing limit order")
            else:
                logger.info(f"{sub} placing {self.repr()}")
            return True
        except ccxt.InvalidOrder as error:
            if "Trigger price" in str(error):
                if logger is None:
                    print(f"ERROR TRIGGER price for {self.symbol} placing limit order")
                else:
                    logger.info(f"{sub} ERROR TRIGGER price while placing {self.repr()}")
                response = exchange.create_order(self.symbol, "limit", self.side, self.size, self.price)
                self.set_order_opening(response)
                return True
        except Exception as e:
            if logger is None:
                print(f"ERROR executing order for {self.symbol}: {self.client_id}, {e}")
            else:
                logger.info(f"{sub} ERROR while placing {self.repr()}")
            try:
                response = exchange.create_order(self.symbol, type, self.side, self.size, self.price, params=self.params)
                self.set_order_opening(response)
                return True
            except:
                self.set_order_opening("failed")
                return False

    def repr(self):
        type = "limit" if "triggerPrice" not in self.params else "stop"
        return f"Order({type}, {self.side}, {self.price})"

    def safe_format(self, exchange: ccxt, size_treshhold: float=2):
        size = self.safe_size(exchange, self.symbol, self.size, size_treshhold)
        if size is None:
            return False
        self.size = size
        self.price = float(exchange.price_to_precision(self.symbol, self.price))
        if self.trigger_price:
            self.trigger_price = float(exchange.price_to_precision(self.symbol, self.trigger_price))
            self.params["triggerPrice"] = self.trigger_price
        return True

    @staticmethod
    def safe_size(exchange: ccxt, symbol, size, size_treshhold:float=2):
        size = float(exchange.amount_to_precision(symbol, size))
        min_size = exchange.markets[symbol]["limits"]["amount"]["min"]
        if size_treshhold * size >= min_size:
            size = size if size > min_size else min_size
            return size
        return None

    @classmethod
    def get_exit(cls, pos: object, profit_target: float = 0):
        if not pos.size > 0:
            return

        exit_price = pos.break_even * (1 + profit_target) if pos.side == "buy" else pos.break_even * (1 - profit_target)
        exit_side = "sell" if pos.side == "buy" else "buy"
        return cls(pos.size, exit_price, exit_side, pos.symbol, CustomId.generate_params(pos.subaccount, pos.symbol, type="RISKEXIT"))


    def parse_update(self, response):
        if isinstance(response, dict) and not self.is_updated:
            if self.status != response["status"]:
                self.status = response["status"]
                self.is_updated = True
            elif self.filled != response["filled"]:
                self.filled = response["filled"]
                self.remaining = response["remaining"]
                self.is_updated = True