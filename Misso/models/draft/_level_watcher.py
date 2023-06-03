import Misso.services.helper as ph
from Misso.services.draft.swing_levels import get_swing_levels
from dataclasses import dataclass, field, InitVar
from typing import Dict

@dataclass
class Levels:
    symbol: str
    last: float
    sell_levels: list
    buy_levels: list
    info: dict
    next_buys: list = field(default_factory=list)
    next_sells: list = field(default_factory=list)
    current_range: list = field(default_factory=list)
    last_range: list = field(default_factory=list)
    level_range: list = field(default_factory=list)
    active_position: bool = False

    def __post_init__(self):
        self.level_range = self.info["range"]
        self.get_next_levels()

    def get_next_levels(self):
        self.next_buys = ph.next_higher_lower_in_array(self.buy_levels, self.last)
        self.next_sells = ph.next_higher_lower_in_array(self.sell_levels, self.last)
        self.current_range = [self.next_buys[0], self.next_sells[1]]

    def update_price(self, current_price):
        if ph.price_in_range(current_price, self.current_range):
            self.last = current_price
        else:
            self.last_range = self.current_range.copy()
            self.get_next_levels()



@dataclass
class Watcher:
    swing_levels: InitVar[dict]
    watch_list: list = field(init=False, default_factory=list)
    markets: Dict[str, Levels] = field(init=False, default_factory=dict)

    def __post_init__(self, swing_levels: dict):
        for symbol, swing_dict in swing_levels.items():
            self.markets[symbol] = Levels(symbol,
                                          swing_dict["last"],
                                          swing_dict["buy_levels"],
                                          swing_dict["sell_levels"],
                                          swing_dict["info"])
        self.watch_list = list(self.markets.keys())

    def clean_watch_list(self):
        watch_list = self.watch_list.copy()
        for symbol in watch_list:
            if symbol not in self.markets:
                self.watch_list.remove(symbol)

    def add_market(self, exchange: object, symbol: str, timeframe: str ="1m", derivate: int=2):
        if symbol not in self.watch_list:
            swing_dict = get_swing_levels(exchange, symbol, timeframe, derivate_num=derivate)
            self.markets[symbol] = Levels(swing_dict["symbol"],
                                          swing_dict["last"],
                                          swing_dict["buy_levels"],
                                          swing_dict["sell_levels"],
                                          swing_dict["info"])

    def parse_tickers(self, price_tickers: dict):
        for symbol, current_price in price_tickers.items():
            self.markets[symbol].update_price(current_price)





