from Misso.models.position.pos_base import PosBase
from Misso.models.position.pos_live import PosLive
from Misso.models.position.pos_risk import PosRisk
from Misso.models.position.pos_wave import PosWave

from dataclasses import dataclass
from Misso.models.meta import iMeta

# @dataclass
# class Position(metaclass=iMeta, modul="Misso.models.position", base_name="PosWave"):
#     base_name: str = None
#     def __post_init__(self):
#         super().__post_init__()


def position_factory(base_name: str):
    assert base_name in globals(), "KeyError Position() base_class unknown"
    base = globals()[base_name]
    return type("Position", (base,),{})



if __name__ == "__main__":
    sym = "BTC/USD:USD"
    base = "PosWave"
    from dataclasses import is_dataclass
    pos = position_factory(base)(sym, "HFT")
    print(is_dataclass(pos))
    print(pos)