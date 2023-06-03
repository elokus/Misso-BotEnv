from dataclasses import dataclass, field
import Misso.services.utilities as utils
from Misso.models.position import PosBase

@dataclass
class PosLive(PosBase):
    side: any = None
    size: float = 0.0
    notional: float = 0.0
    unrealized: float = 0.0
    break_even: float = 0.0

    def __post_init__(self):
        super().__post_init__()

    def update_live(self, response: dict):
        self.size = float(response["contracts"])
        if self.size > 0:
            self.notional = response["notional"]
            self.unrealized = response["unrealizedPnl"]
            self.side = utils.safe_parse_side(response)
            self.break_even = utils.safe_parse_break_even(response)
            self.is_open = True
        else:
            self.reset_live()

    def reset_live(self):
        self.size = 0
        self.notional = 0
        self.unrealized = 0
        self.side = None
        self.break_even = 0
        self.is_open = False

    def export_live(self):
        return {
            "side":self.side,
            "size":self.size,
            "notional":self.notional,
            "unrealized":self.unrealized,
            "break_even":self.break_even,
                }