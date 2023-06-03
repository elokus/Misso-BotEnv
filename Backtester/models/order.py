from dataclasses import dataclass, field
from datetime import datetime
import ccxt


@dataclass
class Order:
    size: float
    price: float
    side: str
    symbol: str
    params: dict = field(default_factory=dict)
    id: str = None
    type: str = "limit"
    client_id: str = None
    status: str = "generated"
    filled: float = 0.0
    _filled: float = 0.0
    trigger_price: float = None
    remaining: float = None
    value: float = None
    is_conditional = False
    closed_at: datetime = None
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
        if self.trigger_price is not None:
            self.is_conditional = True
            self.type = "stop"

    def set_order_opening(self, response: dict=None):
        if isinstance(response, dict):
            self.id = response["id"]
            self.status = "new"
            self.value = response["amount"] * response["price"]
            self.is_placed = True
            self.is_closed = False
            self.is_canceled = False
            self.is_open = True
        if response is None or response == "failed":
            self.status = "failed"

    def set_close(self):
        self.is_closed = True
        self.is_open = False
        self.closed_at = datetime.utcnow()

    def set_cancel(self):
        self.is_open = False
        self.is_closed = True
        self.is_canceled = True

    def create(self, exchange: ccxt):
        if self.is_safe_format(exchange):
            response = self._create(exchange, self.type)
            self.set_order_opening(response)
        return self.is_open

    @decor.safe_order
    def _create(self, exchange: ccxt, type: str="limit"):
        if type == "limit":
            return self.create_limit(exchange)
        if type == "stop":
            return self.create_stop(exchange)

    @decor.safe_request
    def create_limit(self, exchange: ccxt):
        return exchange.create_order(self.symbol, "limit", self.side, self.size, self.price)

    @decor.safe_request
    def create_stop(self, exchange: ccxt):
        return exchange.create_order(self.symbol, "stop", self.side, self.size, self.price, params=self.params)


    def repr(self):
        type = "limit" if "triggerPrice" not in self.params else "stop"
        if type == "stop":
            return f"Order({self.symbol}: {type}-{self.side}, size={self.size}, price={self.price}, trigger={self.params['triggerPrice']}, status={self.status}, id={self.id})"
        return f"Order({self.symbol}: {type}-{self.side}, size={self.size}, price={self.price}, status={self.status}, id={self.id})"

    def repr_short(self):
        return f"Order({self.side}: s={self.size}, p={self.price}, id={self.id})"


    def is_safe_format(self, exchange: ccxt, size_treshhold: float=2):
        size = self.safe_size(exchange, self.symbol, self.size, size_treshhold)
        if size is None:
            self.status = "failed"
            return False
        self.size = size
        self.price = float(exchange.price_to_precision(self.symbol, self.price))
        if self.trigger_price:
            self.trigger_price = float(exchange.price_to_precision(self.symbol, self.trigger_price))
            self.params["triggerPrice"] = self.trigger_price
            self.type = "stop"
        return True

    @staticmethod
    def safe_size(exchange: ccxt, symbol, size, size_treshhold:float=2):
        size = float(exchange.amount_to_precision(symbol, size))
        min_size = exchange.markets[symbol]["limits"]["amount"]["min"]
        if size_treshhold * size >= min_size:
            size = size if size > min_size else min_size
            return size
        return None

    @decor.safe_request
    def check_trigger(self, exchange: ccxt):
        if self.type == "stop":
            oid = exchange.fetch_order_if_from_conditional_order(self.id)
            if oid is not None:
                self.set_triggered(oid)

    def set_triggered(self, oid: str):
        self.id = oid
        self.type = "limit"
        self.trigger_price = None
        self.params = {"type":"limit"}

    def edit_kwargs(self, **kwargs):
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)

    def edit(self, exchange: ccxt, **kwargs):
        self.check_trigger(exchange)
        self.edit_kwargs(**kwargs)
        if self.is_safe_format(exchange):
            response =  self._edit(exchange)
            self.set_order_opening(response)
            return self.status
        return "failed"

    @decor.safe_request
    def _edit(self, exchange: ccxt):
        return exchange.edit_order(self.id, self.symbol, self.type, self.side, self.size, self.price, params=self.params)

    @decor.safe_request
    def cancel(self, exchange: ccxt):
        return exchange.cancel_order(self.id, params={"type":"stop"})
