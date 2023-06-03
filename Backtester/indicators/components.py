from __future__ import annotations
import numpy as np
"""Single action components for wave class"""


def merge_tx_waves(filtered_waves: dict) -> dict:
    def is_in_range(x, mi, mx):
        if mi is None or mx is None:
            return False
        return x >= mi and x <=mx

    waves = []
    indices = []
    mi, mx = None, None

    for tx, wa in filtered_waves.items():
        for idx, w in wa.items():
            if not is_in_range(w, mi, mx):
                waves.append(w)
                indices.append((tx,idx))
        mi = min(waves)
        mx = max(waves)
    return dict(zip(indices, waves))

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
        self.data = self._map_data(data)
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

    def _map_data(self, data: dict):
        return merge_tx_waves(data)

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


