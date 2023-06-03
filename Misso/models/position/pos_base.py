from dataclasses import dataclass
from Misso.models.meta import BaseDC

@dataclass
class PosBase(BaseDC):
    symbol: str
    sub: str = None
    id: str = None
    slot: str = None

    is_open: bool = False
    is_active: bool = False
    def __post_init__(self):
        self.id = self.get_id()

    def get_id(self):
        return f"{self.sub}_{self.symbol}"
