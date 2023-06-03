import numpy as np
from abc import ABC, abstractmethod


WavePair = tuple[float, float]
IdxWavePair = tuple[float, WavePair]
EntryExitPrice = tuple[float, float]


class PriceLevelIndicatorIter(ABC):
    @abstractmethod
    def next(self) -> EntryExitPrice:
        pass

    @abstractmethod
    def reset(self, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def get_reset_queries(self, *args, **kwargs) -> list[dict]:
        pass




#--------------------------------------------------------------



class IterWaves:
    def __init__(self):
        self.data = None

    def lower_pair(self, x: float)-> tuple:
        idx, entry = self._lower(x)
        _, target = self._higher(entry)
        return idx, (entry, target)

    def higher_pair(self, x: float):
        idx, entry = self._higher(x)
        _, target = self._lower(entry)
        return idx, (entry, target)

    def update(self, data: dict):
        self.data = self.flatten(data)
        self._sort_data()

    def _higher(self, x: float):
        for k, v in self.data.items():
            if v > x and int(k[1]) > 0:
                return k, v
        k, v = self._add_higher()
        while v < x:
            k, v = self._add_higher()
        return k, v

    def _lower(self, x: float):
        for k, v in reversed(self.data.items()):
            if v < x and int(k[1]) < 0:
                return k, v
        k, v = self._add_lower()
        while v > x:
            k, v = self._add_lower()
        return k, v

    @staticmethod
    def flatten(data: dict):
        def is_in_range(x, mi, mx):
            if mi is None or mx is None:
                return False
            return x >= mi and x <=mx

        waves = []
        indices = []
        mi, mx = None, None

        for tx, wa in data.items():
            for idx, w in wa.items():
                if not is_in_range(w, mi, mx):
                    waves.append(w)
                    indices.append((tx,idx))
            mi = min(waves)
            mx = max(waves)
        return dict(zip(indices, waves))

    def _sort_data(self):
        self.data = {k: v for k, v in sorted(self.data.items(), key=lambda item: item[1])}

    def _min_data(self):
        return min(self.data, key=self.data.get)

    def _max_data(self):
        return max(self.data, key=self.data.get)

    def _add_lower(self):
        _idx = self._min_data()
        _value = self.data[_idx]
        idx = (_idx[0], _idx[1]-1)
        self.data[idx] = _value - self.mean_dist
        self._sort_data()
        return idx, self.data[idx]

    def _add_higher(self):
        _idx = self._max_data()
        _value = self.data[_idx]
        idx = (_idx[0], _idx[1]+1)
        self.data[idx] = _value + self.mean_dist
        self._sort_data()
        return idx, self.data[idx]

    @property
    def mean_dist(self):
        return np.diff(list(self.data.values())).mean()

class WaveHandler:
    def __init__(self):

        self.data = {}
        self.next = IterWaves()

        self._last_lower = None
        self._last_higher = None

    def update(self, reviews: dict):
        for meta, data in reviews:
            self.data[meta["timeframe"]] = data
        self.next.update(self.data)

    def get_lower(self, last_price)-> IdxWavePair:
        idx, wavepair = self.next.lower_pair(last_price)
        self._last_lower = idx
        return idx, wavepair

    def get_next_lower(self, wp_id):
        idx, wavepair = self.next.lower_pair(self.get_price_by_id(wp_id))
        self._last_lower = idx
        return idx, wavepair


    def get_higher(self, last_price)-> IdxWavePair:
        idx, wavepair = self.next.higher_pair(last_price)
        self._last_higher = idx
        return idx, wavepair

    def get_next_higher(self, wp_id):
        idx, wavepair = self.next.higher_pair(self.get_price_by_id(wp_id))
        self._last_higher = idx
        return idx, wavepair

    def get_price_by_id(self, idx):
        return self.data[idx[0]][idx[1]]


    def _update_data(self):
        for (tx, idx), v in self.next.data.items():
            if tx not in self.data:
                self.data[tx] = {}
            self.data[tx][idx] = v


class WaveIterator(PriceLevelIndicatorIter):
    def __init__(self, side: str, wave_reports: list[tuple], last_price: float):
        self.side = side
        self.data = {}
        self.iter = IterWaves()
        self._metas = []
        self._last_price = None
        self._last_idx = None
        self.reset(wave_reports, last_price)

    def reset(self, reviews: list[tuple], last_price: float):
        self._metas = []
        for meta, data in reviews:
            self._metas.append(meta)
            self.data[meta["timeframe"]] = data
        self.iter.update(self.data)
        self._last_price = last_price

    def next(self):
        if self.side == "buy":
            return self._next_lower()
        else:
            return self._next_higher()

    def get_reset_queries(self, end_tms: int) -> list[dict]:
        queries = []
        for meta in self._metas:
            _meta = {k: v for k, v in meta.items() if k not in ["start", "end"]}
            _meta["end"] = end_tms
            queries.append(_meta)
        return queries

    def _next_lower(self):
        idx, (entry, exit) = self.iter.lower_pair(self._last_price)
        self._last_price = entry
        self._last_idx = idx
        return entry, exit

    def _next_higher(self):
        idx, (entry, exit) = self.iter.higher_pair(self._last_price)
        self._last_price = entry
        self._last_idx = idx
        return entry, exit

    def _update_data(self):
        for (tx, idx), v in self.iter.data.items():
            if tx not in self.data:
                self.data[tx] = {}
            self.data[tx][idx] = v