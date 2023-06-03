import pandas as pd
import numpy as np
from sklearn.cluster import AgglomerativeClustering
from scipy.ndimage import maximum_filter1d, minimum_filter1d


def get_wave_reports(candles_data: list[tuple]) -> list:
    return [get_wave_report(meta, data) for (meta, data) in candles_data]

def get_wave_report(meta: tuple, data: np.ndarray) -> tuple:
    return meta, _get_waves(data)

def get_wave_levels(candles_data: list[tuple]) -> dict:
    d = _get_wave_levels(get_wave_reports(candles_data))
    return {k: v for k, v in sorted(d.items(), key=lambda item: item[1])}

def _get_wave_levels(reports: list[tuple]) -> dict:
    d = {m["timeframe"]: w for m, w in reports}
    return _merge_tx_waves(d)

def _merge_tx_waves(filtered_waves: dict) -> dict:
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
                indices.append((tx, idx))
        mi = min(waves)
        mx = max(waves)
    return dict(zip(indices, waves))

def _get_waves(data: np.ndarray) -> dict:
    try:
        low_waves, high_waves = get_clustered_waves(data, 40, 4)
        waves = filter_waves(wave_dict(low_waves, high_waves))
        return waves
    except:
        return {}

def filter_waves(waves: dict) -> dict:
    """remove -1 and 1 wave from wave_dict"""
    return {k: v for k, v in waves.items() if k not in [-1,1]}

def wave_dict(low_waves: np.ndarray | list, high_waves: np.ndarray | list) -> dict:
    if not isinstance(low_waves, list):
        low_waves = list(low_waves)
    if not isinstance(high_waves, list):
        high_waves = list(high_waves)
    low_waves.reverse()
    d = {}
    for i, w in enumerate(low_waves):
        d[-(i+1)] = w

    for i, w in enumerate(high_waves):
        d[i+1] = w
    return d




def get_cluster(candles, wave_length, num_clusters, target="min"):
    def rolling_min(a, W, fillna=np.nan):
        out_dtype = np.full(0,fillna).dtype
        hW = (W-1)//2 # Half window size
        out = minimum_filter1d(a,size=W, origin=hW)
        if out.dtype is out_dtype:
            out[:W-1] = fillna
        else:
            out = np.concatenate((np.full(W-1,fillna), out[W-1:]))
        return out

    def rolling_max(a, W, fillna=np.nan):
        out_dtype = np.full(0,fillna).dtype
        hW = (W-1)//2 # Half window size
        out = maximum_filter1d(a,size=W, origin=hW)
        if out.dtype is out_dtype:
            out[:W-1] = fillna
        else:
            out = np.concatenate((np.full(W-1,fillna), out[W-1:]))
        return out

    def get_min_waves(ca, wave_length):
        y = rolling_min(ca[:,3], wave_length)
        idxs = np.unique(y, return_index=True)[1]
        y = np.array([y[idx] for idx in sorted(idxs)])
        y = y[~np.isnan(y)]
        y = np.column_stack((y, np.full(len(y), 1)))
        return y

    def get_max_waves(ca, wave_length):
        y = rolling_max(ca[:,2], wave_length)
        idxs = np.unique(y, return_index=True)[1]
        y = np.array([y[idx] for idx in sorted(idxs)])
        y = y[~np.isnan(y)]
        y = np.column_stack((y, np.full(len(y), 1)))
        return y

    if not isinstance(candles, np.ndarray):
        candles = np.array(candles)

    if target == "min":
        x = get_min_waves(candles, wave_length)
    else:
        x = get_max_waves(candles, wave_length)

    # Find Support/Resistance with clustering using the rolling stats
    # Initialize Agglomerative Clustering
    cluster = AgglomerativeClustering(n_clusters=num_clusters, affinity='euclidean', linkage='ward')
    cluster.fit_predict(x)

    waves = np.column_stack((x, cluster.labels_))
    _cluster = np.unique(cluster.labels_)
    if target == "min":
        return np.sort(np.array([np.min(waves[np.where(waves[:,2] == cl)][:,0]) for cl in _cluster]))
    else:
        return np.sort(np.array([np.max(waves[np.where(waves[:,2] == cl)][:,0]) for cl in _cluster]))

def get_clustered_waves(candles, wave_length, num_clusters):
    min_waves = get_cluster(candles, wave_length, num_clusters, target="min")
    max_waves = get_cluster(candles, wave_length, num_clusters, target="max")
    return min_waves, max_waves