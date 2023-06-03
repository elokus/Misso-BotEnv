import os
import Misso.services.utilities as utils
from Misso.models import Order, position_factory
from dataclasses import asdict


_bases = ["Orders", "Positions"]

class StoreOrders:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._order_dir = "orders"
        self.orders_file = "closed_orders.json"
        self.matched_orders_file = "matched_orders.json"
        self.closed_orders = []
        self.START_TX = 1665187200000  #08.10.2022

    def save_orders(self, info: dict, closed_orders: list, matched_orders: dict):
        self.store_closed_orders(info, closed_orders)
        self.store_matched_orders(info, matched_orders)

    def store_closed_orders(self, timestamps: dict, closed_orders: list):
        closed_orders = list(closed_orders) if isinstance(closed_orders, set) else closed_orders
        content= {
            "info":timestamps,
            "closed_orders": closed_orders}
        utils.save_to_json_dict(content, f"{self.order_dir}/{self.orders_file}")

    def store_matched_orders(self,timestamps: dict, matched_orders: dict):
        content={
            "info":timestamps,
            "matched_orders": matched_orders
        }
        utils.save_to_json_dict(content, f"{self.order_dir}/{self.matched_orders_file}")

    def load_orders(self, default_subs=None):
        _co = utils.load_from_json(f"{self.order_dir}/{self.orders_file}")
        _mo = utils.load_from_json(f"{self.order_dir}/{self.matched_orders_file}")
        info = self._compare_timestamps(_co, _mo, default_keys=default_subs)
        return {"info":info,
                "closed_orders": _co["closed_orders"],
                "matched_orders": _mo["matched_orders"]}

    @property
    def order_dir(self):
        return self.storage_dir + "/" + self._order_dir

    def _compare_timestamps(self, cont_a: dict, cont_b: dict, default_keys=None):
        info_a = cont_a["info"] if "info" in cont_a else {}
        info_b = cont_b["info"] if "info" in cont_b else {}
        keys = set(info_a.keys())
        keys.update(info_b.keys())
        if default_keys is not None:
            keys.update(default_keys)
        d = {}
        for key in keys:
            if key in info_a:
                v = int(info_a[key])
                if key in info_b:
                    v = int(info_b[key]) if int(info_b[key]) < v else v
            elif key in info_b:
                v = int(info_a[key])
            else:
                v = self.START_TX
            d[key] = v
        return d

class StorePositions:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.position_dir = "restore"
        self.positions = "positions.json"

    def load_positions(self, subaccount: str):
        _dir = self.get_dir(self.position_dir)
        data = self.get_latest_json(subaccount, _dir)
        return data

    def restore_pos(self, pos_base_class: str, subaccount: str):
        data = self.load_positions(subaccount)
        data = self._restore_pos(data)
        if data is None:
            print(f"No restore position data found for {subaccount}")
            return
        pos = {}
        for market, pos_data in data.items():
            pos[market] = position_factory(pos_base_class)(**pos_data)
        return pos

    def store_positions(self, subaccount: str, positions: dict, run_id: str):
        def factory(data):
            return dict(x for x in data if x[1] is not None)
        content = {}
        for market, pos in positions.items():
            content[market] = asdict(pos, dict_factory=factory)
        utils.save_to_json_dict(content, file=self._pos_file_name(subaccount, run_id), dir=self.get_dir(self.position_dir))

    def _pos_file_name(self, subaccount: str, suffix: str):
        return f"{subaccount}_states_{suffix}.json"

    def _restore_pos(self, data: dict):
        if data is not None:
            _data = data.copy()
            for market, pos_data in _data.items():
                data[market]["orders"] = self._restore_pos_orders(pos_data["orders"])
                data[market]["curr_wave_pairs"] = self._safe_string(pos_data["curr_wave_pairs"])
        return data

    def _safe_string(self, d: dict):
        r = {}
        for k, v in d.items():
            if "-" in k:
                _k = int(k.split("-")[1]) * -1
            else:
                _k = int(k)
            r[_k] = v
        return r

    def _restore_pos_orders(self, orders: dict):
        if len(orders) > 0:
            d = {}
            for k, o in orders.items():
                d[k] = Order(**o)
            return d
        return orders





class Meta(type):
    def __new__(cls, name, base, dct):
        pot_bases = ["Positions", "Orders", "Watcher", "Submanager", "Watch_list", "States", "Market_review"]
        _bases = []
        for pot in pot_bases:
            if Meta.is_base(pot):
                _bases.append(globals()[f"Store{pot}"])
        _bases = tuple(_bases)
        print(_bases)
        return type(name, _bases, dct)

    @staticmethod
    def is_base(basename: str):
        return basename in _bases



class Store(metaclass=Meta):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage_dir = "storage"
        self.files = []
        self.init_storage_path()

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

    def get_latest_json(self, prefix: str, dir: str, remove_old: bool=False):
        try:
            files = [f for f in os.listdir(dir) if f.startswith(prefix)]
            files.sort()
            file = f"{dir}/{files[-1]}"
            if remove_old:
                self.remove_old_files(files, dir, file)
            data = utils.load_from_json(file)
            return data
        except:
            print(f"NO FILE starting with {prefix} found in {dir}")
            return None

    def remove_old_files(self, files: list, dir: str, exclude: str=""):
        for file in files:
            _file = f"{dir}/{file}"
            if _file != exclude and os.path.exists(_file):
                os.remove(_file)

    def get_dir(self, sub_dir: str):
        return os.path.join(self.storage_dir, sub_dir)



if __name__=="__main__":
    store = Store()
    print(store.files)
    print(store.orders)
    print(store.positions)