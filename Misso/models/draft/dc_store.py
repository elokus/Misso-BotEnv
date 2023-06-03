import os
from dataclasses import dataclass, field
from Misso.models.meta import Meta

meta_config = ["StoreOrders", "StorePositions"]


@dataclass
class Store(metaclass=Meta, bases=meta_config, base_modul="Misso.models.StoreBases"):
    storage_dir: str = "storage"
    files: list = field(default_factory=list)


    def __post_init__(self):
        super().__post_init__()
        self.init_store_files()

    def init_storage_path(self, main="Misso"):
        p = os.getcwd()
        i = ""
        while os.path.split(p)[1] != main:
            i += "..\\"
            p = os.path.split(p)[0]
        self.storage_dir = f"{i}{self.storage_dir}"

    def init_store_files(self):
        attributes = ["positions", "orders", "watcher", "submanager", "watch_list", "states", "market_review"]
        files = [self.get_file(attr) for attr in attributes]
        self.files = [file for file in files if file is not None]

    def get_file(self, attribute: str):
        if hasattr(self, attribute):
            return getattr(self, attribute)
        return None





if __name__=="__main__":
    store = Store()
    print(store.files)
    print(store.orders)
    print(store.positions)