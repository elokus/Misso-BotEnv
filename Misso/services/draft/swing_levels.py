import numpy as np
import time
import Misso.services.helper as ph
import Misso.services.async_helper as ah
import asyncio

##################################################
###
###  HIGH LEVEL FUNCTION:
###
##################################################

async def a_get_swing_levels(exchange: object, symbol: str, timeframe: str="1m", limit: int=5000, derivate_num: int=2, ref_price: float=None, percentile=70):
    valid_levels = False
    precision, step, last = await a_precision_step_from_symbol(exchange, symbol)
    while not valid_levels:
        try:
            highs, lows = await a_get_high_low_derivate(exchange, symbol, timeframe, limit, derivate_num, ref_price)
            cycle_time = _get_cycle_time(highs, lows)
            lvl_vol_high = get_grouped_swing_volume(highs, precision)
            lvl_high = list(get_filtered_lvl_vol(lvl_vol_high, percentile, step).keys())
            lvl_vol_low = get_grouped_swing_volume(lows, precision)
            lvl_low = list(get_filtered_lvl_vol(lvl_vol_low, percentile, step).keys())
            lvl_range = [min(lvl_low), max(lvl_high)]
            if ref_price is not None:
                if ref_price > lvl_range[0] and ref_price < lvl_range[1]:
                    valid_levels = True
                else:
                    timeframe = ph.shift_timeframe(timeframe, 1)
            else:
                valid_levels = True
        except:
            print(f"ERROR for {symbol} at timeframe: {timeframe} trying next higher timeframe")
            timeframe = ph.shift_timeframe(timeframe, 1)
    info = {"range": lvl_range, "precision": precision, "step":step, "timeframe": timeframe, "cycle_time": cycle_time, "derivate":derivate_num, "limit":limit, "ref_price":last}
    if not is_valid_level_count(lvl_high, lvl_low, last):
        lvl_low, lvl_high, info = await a_extend_levels(exchange, symbol, info, lvl_low, lvl_high)
    return dict(symbol=symbol, buy_levels=lvl_low, sell_levels=lvl_high, info=info, last=last)

def get_swing_levels(exchange: object, symbol: str, timeframe: str="1m", limit: int=5000, derivate_num: int=2, ref_price: float=None, percentile=70):
    valid_levels = False
    precision, step, last = a_precision_step_from_symbol(exchange, symbol)
    while not valid_levels:
        try:
            highs, lows = get_high_low_derivate(exchange, symbol, timeframe, limit, derivate_num, ref_price)
            cycle_time = _get_cycle_time(highs, lows)
            lvl_vol_high = get_grouped_swing_volume(highs, precision)
            lvl_high = list(get_filtered_lvl_vol(lvl_vol_high, percentile, step).keys())
            lvl_vol_low = get_grouped_swing_volume(lows, precision)
            lvl_low = list(get_filtered_lvl_vol(lvl_vol_low, percentile, step).keys())
            lvl_range = [min(lvl_low), max(lvl_high)]
            if ref_price is not None:
                if ref_price > lvl_range[0] and ref_price < lvl_range[1]:
                    valid_levels = True
                else:
                    timeframe = ph.shift_timeframe(timeframe, 1)
            else:
                valid_levels = True
        except:
            print(f"ERROR for {symbol} at timeframe: {timeframe} trying next higher timeframe")
            timeframe = ph.shift_timeframe(timeframe, 1)
    info = {"range": lvl_range, "precision": precision, "step":step, "timeframe": timeframe, "cycle_time": cycle_time, "derivate":derivate_num, "limit":limit, "ref_price":last}
    if not is_valid_level_count(lvl_high, lvl_low, last):
        lvl_low, lvl_high, info = extend_levels(exchange, symbol, info, lvl_low, lvl_high)
    return dict(symbol=symbol, buy_levels=lvl_low, sell_levels=lvl_high, info=info, last=last)




### HELPER:
def _precision_step_from_symbol(exchange, symbol, i=4):
    x = exchange.fetch_ticker(symbol)["last"]
    while x/(1/10**i) > 1000:
        i -= 1
    return i, 1/10**i, x

async def a_precision_step_from_symbol(exchange, symbol, i=4):
    x = await exchange.fetch_ticker(symbol)
    x = x["last"]
    while x/(1/10**i) > 1000:
        i -= 1
    return i, 1/10**i, x





### GET HIGH LOW DERIVATE
###

### HELPER:

def _filter_swings_volume(candles, swing_type="lows", input_type="candles"):
    if input_type == "derivate":
        array = candles[0]
        volume = candles[1]
        timestamp = candles[2]
    else:
        if swing_type == "lows" or swing_type == "low":
            array = candles[:,3]
            volume = candles[:,5]
            timestamp = candles[:, 0]
        elif swing_type == "highs" or swing_type == "high":
            array = candles[:,2]
            volume = candles[:,5]
            timestamp = candles[:,0]
        else:
            print("ERROR filter_swing_volume() select swing type [highs, lows]")
            return
    a = np.diff(array)
    n = np.sign(a)
    a = np.roll(n, shift=1)
    highs = []
    lows = []
    for i in range(len(a)):
        if i == 0:
            highs.append(0)
            lows.append(0)

        elif a[i] == 1 and n[i] != 1:
            highs.append(1)
            lows.append(0)
        elif a[i] == -1 and n[i] != -1:
            highs.append(0)
            lows.append(1)
        else:
            highs.append(0)
            lows.append(0)
    high_prices = array[np.where(np.array(highs) == 1)]
    high_volume = volume[np.where(np.array(highs) == 1)]
    high_time = timestamp[np.where(np.array(highs) == 1)]
    low_prices = array[np.where(np.array(lows) == 1)]
    low_volume = volume[np.where(np.array(lows) == 1)]
    low_time = timestamp[np.where(np.array(lows) == 1)]
    if swing_type == "highs" or swing_type == "high":
        return [high_prices, high_volume, high_time]
    if swing_type == "lows" or swing_type == "low":
        return [low_prices, low_volume, low_time]

def _aggregate_swings_from_candles(candles):
    highs = _filter_swings_volume(candles, swing_type="highs")
    lows = _filter_swings_volume(candles, swing_type="lows")
    return highs, lows

def _high_low_derivate(candles, derivate_num=2):
    if isinstance(candles, list):
        candles = np.array(candles)
    highs, lows = _aggregate_swings_from_candles(candles)
    for i in range(derivate_num):
        highs = _filter_swings_volume(highs, swing_type="highs", input_type="derivate")
        lows = _filter_swings_volume(lows, swing_type="lows", input_type="derivate")
    return highs, lows

def _fetch_candles(exchange, symbol, timeframe, limit=5000):
    if limit <= 5000:
        candles = np.array(exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit))
    else:
        since = (int(time.time()) - ph.timeframe_string_to_seconds(timeframe) * limit) *1000
        candles = np.array(ph.loop_fetch_candles(exchange, symbol, timeframe, since=since, limit=limit))
    return candles

def _prepare_candles(exchange, symbol, timeframe, limit=5000, ref_price=None):
    if ref_price is not None:
        candles = _fetch_candles(exchange, symbol, timeframe, limit)
        i = 1
        while ref_price *0.98 < np.min(candles[:,3]) or ref_price*1.02 > np.max(candles[:,2]):
            _timeframe = ph.shift_timeframe(timeframe, i)
            candles = _fetch_candles(exchange, symbol, _timeframe, limit)
            i += 1
            if i > 4:
                print("ERROR _prepare_candles() failed ref_price out of candles range 1h")
                break
    else:
        candles = _fetch_candles(exchange, symbol, timeframe, limit)
    return candles

async def a_fetch_candles(exchange, symbol, timeframe, limit=5000):
    if limit <= 5000:
        candles = await exchange.fetchOHLCV(symbol, timeframe=timeframe, limit=limit)
    else:
        since = (int(time.time()) - ph.timeframe_string_to_seconds(timeframe) * limit) *1000
        candles = await ah.loop_fetch_candles(exchange, symbol, timeframe, since=since, limit=limit)

    return np.array(candles)

async def a_prepare_candles(exchange, symbol, timeframe, limit=5000, ref_price=None):
    if ref_price is not None:
        candles = await a_fetch_candles(exchange, symbol, timeframe, limit)
        i = 1
        while ref_price *0.98 < np.min(candles[:,3]) or ref_price*1.02 > np.max(candles[:,2]):
            _timeframe = ph.shift_timeframe(timeframe, i)
            candles = await a_fetch_candles(exchange, symbol, _timeframe, limit)
            i += 1
            if i > 4:
                print("ERROR _prepare_candles() failed ref_price out of candles range 1h")
                break
    else:
        candles = await a_fetch_candles(exchange, symbol, timeframe, limit)
    return candles
###
### ---> COMBINED:

def get_high_low_derivate(exchange: object, symbol: str, timeframe: str="1m", limit: int=5000, derivate_num: int=2, ref_price: float=None):
    candles = _prepare_candles(exchange, symbol, timeframe, limit, ref_price)
    highs, lows = _high_low_derivate(candles, derivate_num)
    return highs, lows

async def a_get_high_low_derivate(exchange: object, symbol: str, timeframe: str="1m", limit: int=5000, derivate_num: int=2, ref_price: float=None):
    candles = await a_prepare_candles(exchange, symbol, timeframe, limit, ref_price)
    highs, lows = _high_low_derivate(candles, derivate_num)
    return highs, lows
# ------------------------------------------------------------------------------------


### GET GROUPED SWING VOLUME
###

### HELPER:

def _group_unique_swing_levels(swings: list, precision: int=2):
    prices, volume = swings[0], swings[1]
    prices = np.around(prices, decimals=precision)
    uni_data = np.unique(prices, return_inverse = True)
    return uni_data #uni, idx

def _grouped_swing_volume(swings, uni, idx):
    lvls, vol = swings[0], swings[1]
    d = {i : [] for i in uni}
    for vol_idx, uni_ix in np.ndenumerate(idx):
        d[uni[uni_ix]].append(vol[vol_idx])
    return d

def _sum_grouped_swing_volume_list(swing_volume_dict: dict):
    d = {}
    for lvl, vols in swing_volume_dict.items():
        d[lvl] = round(sum(vols),2)
    return d

###
### ---> COMBINED:

def get_grouped_swing_volume(swings: list, precision: int=2):
    uni_data = _group_unique_swing_levels(swings, precision=precision)
    lvl_vol = _grouped_swing_volume(swings, *uni_data)
    lvl_vol = _sum_grouped_swing_volume_list(lvl_vol)
    return lvl_vol

# ------------------------------------------------------------------------------------


### GET FILTERED LEVEL VOLUME
###

### HELPER:

def _filter_lvl_dict_percentile(d: dict, q: int):
    a = np.array(list(d.values()))
    perc = np.percentile(a, q)
    _d = {}
    for k, v in d.items():
        if v >= perc:
            _d[round(k,2)] = v
    return _d

def _filter_lvl_dict_neighbor(d: dict, lvl_range: list, step=0.01, return_all_steps=False):
    d = _enum_key_steps_dict(d, lvl_range, step=step)
    _d = {}
    last = 0
    last_key = None
    for key, value in d.items():
        if last > 0:
            if value > last:
                _d[key] = value
                if not last_key is None:
                    _d[last_key] = 0
            else:
                _d[key] = 0
        else:
            _d[key] = value
        last = value
        last_key = key
    if return_all_steps:
        return _d
    return {key: value for key, value in _d.items() if value > 0}

def _enum_key_steps_dict(lvl_dict: dict, lvl_range: list,  step: float=0.01):
    x = np.arange(lvl_range[0], lvl_range[1], step)
    x_d = {}
    for i in x:
        j = round(i, 2)
        x_d[j] = 0 if not j in lvl_dict else lvl_dict[j]
    return x_d

def _range_from_dict_keys(_dict: dict):
    l = [float(x) for x in _dict.keys()]
    return [min(l), max(l)]

###
### ---> COMBINED:

def get_filtered_lvl_vol(lvl_vol: dict, percentile: int=70, step: float=0.01, lvl_range: list=None, return_range=False):
    lvl_range = _range_from_dict_keys(lvl_vol) if lvl_range is None else lvl_range
    lvl_vol = _filter_lvl_dict_percentile(lvl_vol, percentile)
    lvl_vol = _filter_lvl_dict_neighbor(lvl_vol, lvl_range, step=step)
    if return_range:
        return lvl_vol, lvl_range
    return lvl_vol

# ------------------------------------------------------------------------------------


### PLOT FILTERED LEVEL DICT
###

def generate_plot_dict(lvl_dict: dict, lvl_range: list,  step: float=0.01, return_x_y=False, keys_as_string=True, add_q=False, sub_q=False):
    x = np.arange(lvl_range[0], lvl_range[1], step)
    x_d = {}
    for i in x:
        j = round(i, 2)
        j = j + step/4 if add_q else j
        j = j - step/4 if sub_q else j
        key = str(j) if keys_as_string else j
        x_d[key] = 0 if not j in lvl_dict else lvl_dict[j]
    if return_x_y:
        return list(x_d.keys()), list(x_d.values())
    return x_d

def plot_lvl_dict(lvl_dict: dict, lvl_range: list=None, x_step: float =0.01, vertical: bool=False):
    import matplotlib.pyplot as plt
    if lvl_range is None:
        lvl_range = [min(lvl_dict.keys()), max(lvl_dict.keys())]
    x, y = generate_plot_dict(lvl_dict, lvl_range, step=x_step, return_x_y=True)
    plt.figure(figsize=(20, 12))
    if vertical:
        plt.barh(x, y)
    else:
        plt.bar(x, y)
    plt.show()

def plot_multi_lvl_dict(a_lvl: dict, b_lvl: dict, lvl_range: list=None, x_step: float =0.01, vertical: bool=True):
    import matplotlib.pyplot as plt
    if lvl_range is None:
        lvl_range = [min([min(a_lvl.keys()), min(b_lvl.keys())]), max([max(a_lvl.keys()), max(b_lvl.keys())])]
    x_a, y_a = generate_plot_dict(a_lvl, lvl_range, step=x_step, return_x_y=True, add_q=True)
    x_b, y_b = generate_plot_dict(b_lvl, lvl_range, step=x_step, return_x_y=True, sub_q=True)

    plt.figure(figsize=(20, 12))
    if vertical:
        plt.barh(x_a, y_a, label="lows")
        plt.barh(x_b, y_b, label= "highs")
    else:
        plt.bar(x_a, y_a, label="lows")
        plt.bar(x_b, y_b, label= "highs")
    plt.show()

# ------------------------------------------------------------------------------------


### GET CYCLE TIME
###


def get_cycle_time(exchange, symbol, derivate_num=2, return_minutes=True, limit=5000, return_array=False):
    highs, lows = get_high_low_derivate(exchange, symbol, limit=limit, derivate_num=derivate_num)
    t = np.hstack((highs[2], lows[2]))
    a = np.column_stack((t, np.hstack((highs[0], lows[0]))))
    a = a[a[:,0].argsort()]
    a = np.column_stack((a, np.sign(a[:,1] - np.roll(a[:,1], shift = -1))))
    a = a[np.where(a[:,2] != np.roll(a[:,2], shift=-1))]
    _a = a[np.where(a[:,2] == 1)]
    dtime = np.mean(np.diff(_a[:,0]))/60000 if return_minutes else np.mean(np.diff(_a[:,0]))/1000
    if return_array:
        return dtime, a
    return dtime

def _get_cycle_time(highs, lows, return_minutes=True, return_array=False):
    t = np.hstack((highs[2], lows[2]))
    a = np.column_stack((t, np.hstack((highs[0], lows[0]))))
    a = a[a[:,0].argsort()]
    a = np.column_stack((a, np.sign(a[:,1] - np.roll(a[:,1], shift = -1))))
    a = a[np.where(a[:,2] != np.roll(a[:,2], shift=-1))]
    _a = a[np.where(a[:,2] == 1)]
    dtime = np.mean(np.diff(_a[:,0]))/60000 if return_minutes else np.mean(np.diff(_a[:,0]))/1000
    if return_array:
        return dtime, a
    return dtime


def df_from_cycle_array(a):
    df = pd.DataFrame({"time": a[:,0], "levels": a[:,1]})
    df["time"] = pd.to_datetime(df["time"], unit="ms")
    return df.set_index("time", drop=True)

def count_levels_higher(levels, ref_price):
    i = 0
    for lvl in levels:
        if lvl > ref_price:
            i += 1
    return i

def count_levels_lower(levels, ref_price):
    i = 0
    for lvl in levels:
        if lvl < ref_price:
            i += 1
    return i

def is_valid_level_count(high_levels, low_levels, ref_price, min_levels=5):
    if count_levels_lower(low_levels, ref_price) > min_levels and count_levels_higher(high_levels, ref_price) > min_levels:
        return True
    return False


async def a_extend_levels(exchange, symbol, info, old_low, old_high):
    next_timeframe = ph.shift_timeframe(info["timeframe"], 1)
    limit = info["limit"]
    derivate_num = info["derivate"]
    old_range = info["range"]
    highs, lows = await a_get_high_low_derivate(exchange, symbol, next_timeframe, limit, derivate_num)
    lvl_vol_high = get_grouped_swing_volume(highs, info["precision"])
    lvl_high = list(get_filtered_lvl_vol(lvl_vol_high, info["step"]).keys())
    lvl_vol_low = get_grouped_swing_volume(lows, info["precision"])
    lvl_low = list(get_filtered_lvl_vol(lvl_vol_low, info["step"]).keys())
    lvl_range = [min(lvl_low), max(lvl_high)]
    lvl_high = list(filter(lambda x: x > old_range[1] or x < old_range[0], lvl_high))
    lvl_low = list(filter(lambda x: x > old_range[1] or x < old_range[0], lvl_low))
    lvl_high.extend(old_high)
    lvl_high.sort()
    lvl_low.extend(old_low)
    lvl_low.sort()
    info["range"] = lvl_range
    info["timeframe"] = next_timeframe
    return lvl_low, lvl_high, info


def extend_levels(exchange, symbol, info, old_low, old_high):
    next_timeframe = ph.shift_timeframe(info["timeframe"], 1)
    limit = info["limit"]
    derivate_num = info["derivate"]
    old_range = info["range"]
    highs, lows = get_high_low_derivate(exchange, symbol, next_timeframe, limit, derivate_num)
    lvl_vol_high = get_grouped_swing_volume(highs, info["precision"])
    lvl_high = list(get_filtered_lvl_vol(lvl_vol_high, info["step"]).keys())
    lvl_vol_low = get_grouped_swing_volume(lows, info["precision"])
    lvl_low = list(get_filtered_lvl_vol(lvl_vol_low, info["step"]).keys())
    lvl_range = [min(lvl_low), max(lvl_high)]
    lvl_high = list(filter(lambda x: x > old_range[1] or x < old_range[0], lvl_high))
    lvl_low = list(filter(lambda x: x > old_range[1] or x < old_range[0], lvl_low))
    lvl_high.extend(old_high)
    lvl_high.sort()
    lvl_low.extend(old_low)
    lvl_low.sort()
    info["range"] = lvl_range
    info["timeframe"] = next_timeframe
    return lvl_low, lvl_high, info
