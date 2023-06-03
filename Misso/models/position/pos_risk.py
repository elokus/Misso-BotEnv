from dataclasses import dataclass
from Misso.models.position import PosBase

@dataclass
class PosRisk(PosBase):
    value: int=200

    def __post_init__(self):
        super().__post_init__()