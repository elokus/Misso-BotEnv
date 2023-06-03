from dataclasses import dataclass
from Misso.models.meta import BaseDC



@dataclass
class StoreOrders(BaseDC):
    orders: str="closed_orders.json"

    def __post_init__(self):
        super().__post_init__()

@dataclass
class StorePositions(BaseDC):
    positions: str = "positions.json"

    def __post_init__(self):
        super().__post_init__()