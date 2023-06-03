from dataclasses import dataclass, field
from itertools import count
from datetime import datetime
from typing import List, Dict

counter = count()

@dataclass
class Order:
    id: str
    size: float
    price: float
    side: str
    symbol: str
    status: str = "open"
    filled: float = 0.0
    _filled: float = 0.0
    remaining: float = field(init=False)
    value: float = field(init=False)
    opened_at: datetime = field(default_factory = datetime.utcnow)
    index: int = field(default_factory=lambda: next(counter))
    closed_at: int = None
    is_processed: bool = False
    is_partially_filled: bool = False
    is_updated: bool = False


    def __post_init__(self):
        self.remaining = self.size
        self.value = self.size * self.price

    def parse_update(self, response):
        if isinstance(response, dict) and not self.is_updated:
            if self.status != response["status"]:
                self.status = response["status"]
                self.is_updated = True
            elif self.filled != response["filled"]:
                self.filled = response["filled"]
                self.remaining = response["remaining"]
                self.is_updated = True


