from dataclasses import dataclass, field
import time
from Misso.services.helper import parse_side, parse_exit_side


@dataclass
class Position:
    symbol: str
    size_increment: float
    min_amount: float
    price_increment: float
    side: str = None
    size: float = 0.0
    net_size: float = 0.0
    last_net: float = 0.0
    dca_count: int = 0
    notional: float = 0.0
    unrealized: float = 0.0
    break_even: float = 0.0
    buy_order: list = field(default_factory=list)
    buy_status: str = "closed"
    fills: list = field(default_factory=list)
    sell_order: list = field(default_factory=list)
    sell_status: str = "closed"
    timestamp: int = int(time.time())*1000
    current_range: dict = field(default_factory=dict)
    current_min_max: list = field(default_factory=list)
    order_size: float = 0.0
    is_open: bool = False
    is_pending: bool = False

    def update(self, response):
        self.side = parse_side[response["side"]]
        self.size = response["contracts"]
        last_net_size = self.net_size
        self.net_size = self.size *1 if self.side == "buy" else self.size * -1
        self.last_net = last_net_size if last_net_size != self.net_size else self.last_net
        self.notional = response["notional"]
        self.unrealized = response["unrealizedPnl"]
        if not self.notional is None and not self.unrealized is None:
            self.break_even = (self.notional + self.unrealized)/self.size
        self.is_open = True if self.size > 0 else False
        if not self.is_pending:
            pass


    def update_orders(self, buy_order: list = None, sell_order: list = None):
        if buy_order is not None:
            self.buy_order = buy_order if buy_order[0] != "failed" else self.buy_order
        if sell_order is not None:
            self.sell_order = sell_order if sell_order[0] != "failed" else self.sell_order
        if len(self.fills) > 0:
            for fill in self.fills:
                self.buy_order[0] = "closed" if fill["orderId"] == self.buy_order[0] else self.buy_order[0]
                self.sell_order[0] = "closed" if fill["orderId"] == self.sell_order[0] else self.sell_order[0]

    async def a_update_orders(self, exchange: object):
        import Misso.services.async_helper as ah
        if len(self.buy_order) > 0:
            if self.buy_order[0] != "closed" and self.buy_order[0] != "canceled":
                buy_status = await ah.get_order_status(exchange, self.buy_order[0])
                print(f"Buy order status {self.symbol} is {buy_status}")
                if buy_status == "closed" or buy_status == "canceled":
                    self.buy_order[0] = buy_status
            else:
                print(f"Buy order status {self.symbol} is {self.buy_order[0]}")
        else:
            print(f"No buy_order found for {self.symbol}")

        if len(self.sell_order) > 0:
            if self.sell_order[0] != "closed" and self.sell_order[0] != "canceled":
                sell_status = await ah.get_order_status(exchange, self.sell_order[0])
                print(f"Sell order status {self.symbol} is {sell_status}")
                if sell_status == "closed" or sell_status == "canceled":
                    self.sell_order[0] = sell_status
            else:
                print(f"Sell order status {self.symbol} is {self.sell_order[0]}")
        else:
            print(f"No sell_order found for {self.symbol}")

    def is_filled(self):
        return self.buy_order[0] == "closed" or self.sell_order[0] == "closed"

