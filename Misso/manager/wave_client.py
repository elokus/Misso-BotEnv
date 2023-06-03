import ccxt, queue
from collections import deque
from Misso.manager.client import SubClient
from Misso.models.objects import Position
from Misso.models.store import Store
from Misso.models.streamer.market_streamer import MarketStreamer
import Misso.services.utilities as utils
import Misso.services.cust_decorator as decor

class WaveClient(SubClient):
    def __init__(self, subaccount, **kwargs):

        self._exchange: ccxt=None

        self.RESTORE = True
        self.TIMEFRAMES = ["1m", "15m", "1h", "4h"]
        self.SIZE_INCREMENT = 4
        self.BASE_VALUE = 0
        self.BASE_TIMEFRAME = "1m"
        self.MAX_TIMEFRAME = "4h"
        self.TARGET_BY = "next"
        self.MAX_VALUE = 40
        self.TARGET_FACTOR = 0.5
        self.RISK_VALUE = 30
        self.POSITION_BASE = "PosWave"
        self.SLOTS = {"buy":10, "sell":10, "both":10}
        self.store = None

        super().__init__(subaccount, **kwargs)

        self.store = self.store if self.store is not None else Store()
        self._orders = []
        self._ranges = []
        self.slot_ranks = {}
        self.slots = queue.Queue()
        self.active_watcher = {}
        self.init_client()

    def init_strategy(self):
        if self.market_streamer is None:
            self.market_streamer = MarketStreamer(run_id=self.name, logger=self.logger)
            self.market_streamer.run()
        self.restricted = set(self.restricted)

    def handle_slots(self):
        while not self.slots.empty():
            slot = self.slots.get()
            pos = self.get_next_ranked_pos(slot)
            if pos is not None:
                self.activate_pos(pos, slot)

    def handle_active(self):
        active = self.active_pos
        _active = [pos.symbol for pos in active]
        self.log(f"active pos: {_active}")
        for pos in self.active_pos:
            if pos.has_breakout:
                self.handle_breakout(pos)
            if pos.has_closed_op:
                self.handle_closed_op(pos)

    def handle_breakout(self, pos: Position):
        pos = self.update_pos(pos)
        self.log(f"handle breakout for {pos.symbol}, open: {pos.is_open}, side:{pos.side}")
        if pos.is_open:
            if pos.side != pos.breakout:
                pos.slot = pos.side
                self.handle_pos_upgrade(pos)
        else:
            self.slots.put(pos.slot)
            pos.reset(self.exchange)
        pos.breakout = None
        pos.has_breakout = False

    def handle_pos_upgrade(self, pos: Position):
        if self.is_below_max_timeframe(pos):
            pos.upgrade_timeframe()
            self.activate_pos(pos, pos.side)
        else:
            self.add_risk_position(pos)

    def handle_pos_downgrade(self, pos: Position):
        if pos.curr_txi > 0:
            pos = self.update_pos(pos)
            if not pos.is_open:
                self.slots.put(pos.slot)
                pos.reset(self.exchange)
                self.remove_from_watcher(pos)

    def handle_closed_op(self, pos: Position):
        while len(pos._closed_op) > 0:
            op_id = pos._closed_op.pop()
            idx, tx = float(op_id.split("_")[0]), op_id.split("_")[1]
            if pos.base_tx != tx:
                if pos.has_open_ops:
                    pos.replace_order_pair(self.exchange, idx, tx, self.logger)
                else:
                    self.handle_pos_downgrade(pos)
            else:
                pos.replace_order_pair(self.exchange, idx, tx, self.logger)
        pos.has_closed_op = False

    def handle_out_msgs(self):
        for pos in self.active_pos:
            while len(pos._orders) > 0:
                oid = pos._orders.pop()
                self.watch_order_id(oid, pos.symbol)

    def activate_pos(self, pos: Position, slot: str):
        self.log(f"activating position {pos.symbol} with slot: {slot}")
        pos.cancel_order_pairs(self.exchange)
        pos.update_wave_range(self.market_streamer.reviews)
        target_factor = 0.3 if pos.curr_txi > 0 else self.TARGET_FACTOR
        pos.update_wave_pairs(self.TARGET_BY, target_factor)
        pos.place_order_pairs(self.exchange, slot, self.logger)
        pos.slot = slot
        pos.is_active = True
        self.add_to_watcher(pos)

    @decor.with_semaphore
    def cleaning_routine(self):
        self.log("cleaning routine started")
        self.clean_positions()
        self.update_slot_ranks()
        self.release_slots()
        self.save_positions()
        self.set_next_clean()

    def stop_routine(self):
        pass

    def range_break_low(self, msg):   # msg: sub, "low_breakout", market, watch_job
        self.pos[msg[2]].breakout = "sell"
        self.pos[msg[2]].has_breakout = True

    def range_break_high(self, msg):  # msg: sub, "high_breakout", market, watch_job
        self.pos[msg[2]].breakout = "buy"
        self.pos[msg[2]].has_breakout = True

    def order_closing(self, msg):    # msg: sub, "order_closed", market, (id, order)
        self.pos[msg[2]].parse_closed_order(msg[3])

    def remove_risk_position(self, msg):
        pass

    def add_risk_position(self, pos: Position):
        self.log(f"adding risk position {pos.symbol}")
        pos.is_risk = True
        pos.reset(self.exchange)
        self.delegate_risk_position(pos)


    def intervention_handler(self, msg):
        if msg[2] == "close_long":
            self.close_pos_by_side("buy")
        elif msg[2] == "close_short":
            self.close_pos_by_side("sell")
        elif msg[2] == "close_all":
            self.close_pos_by_side()



    def _position_params(self, market):
        return {"sub": self.sub,
                "timeframes":self.TIMEFRAMES,
                "base_tx": self.BASE_TIMEFRAME,
                "base_value":self.BASE_VALUE,
                "base_size": utils.safe_value_to_size(self.exchange, market, self.BASE_VALUE),
                "size_increment": self.SIZE_INCREMENT}


    def _restore_client(self):
        pos = self.store.restore_pos(self.POSITION_BASE, self.sub)
        if pos is not None:
            self.pos = pos
            for pos in self.active_pos:
                pos._orders = list(pos.orders.keys())
                self.add_to_watcher(pos)
            


    def release_slots(self):
        blocked = self.blocked_slots.copy()
        self.log(f"RELEASE SLOTS: {self.SLOTS} - - - blocked: {self.blocked_slots}")
        for k, v in self.SLOTS.items():
            n = v - blocked[k] if k in blocked else v
            if n <= 0:
                continue
            for i in range(n):
                self.slots.put(k)

    def update_slot_ranks(self):
        buys, sells, both = self.market_streamer.get_ranked(target_tx=self.BASE_TIMEFRAME, skip_buys=self.get_active("buy"), skip_sells=self.get_active("sell"), skip_both=self.get_active("both"))
        self.slot_ranks = {"buy": deque(buys), "sell": deque(sells), "both":deque(both)}

    def get_active(self, slot: str):
        return [market for market, pos in self.pos.items() if pos.slot == slot]

    def _get_next_ranked_market(self, slot: str):
        market = self.slot_ranks[slot].popleft() if len(self.slot_ranks[slot]) > 0 else None
        return market

    def get_next_ranked_pos(self, slot: str):
        market = self._get_next_ranked_market(slot)
        pos = self.pos[market] if market in self.pos else None
        return pos

    def clean_positions(self):
        open_pos = self.fetch_open_positions().copy()
        open_markets = [pos["symbol"] for pos in open_pos]
        for pos in open_pos:
            self.pos[pos["symbol"]].update_live(pos)
        for market, pos in self.pos.items():
            if market not in open_markets:
                if pos.is_active:
                    self.remove_from_watcher(pos)
                pos.reset(self.exchange)


    def close_pos_by_side(self, side=None):
        self.clean_positions()
        open_pos = self.open_pos.copy()
        for pos in open_pos:
            if side is None or pos.side == side:
                pos.market_close(self.exchange)

    def is_below_max_timeframe(self, pos: Position):
        return utils.index_by_timeframe(self.MAX_TIMEFRAME, self.TIMEFRAMES) > utils.index_by_timeframe(pos.curr_tx, self.TIMEFRAMES)