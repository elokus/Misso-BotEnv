from loguru import logger


class OrderHandler:
    def __init__(self, symbol: str, sub: str, **kwargs):
        super().__init__(symbol, sub)
        self.active = {}
        self.pairs = {}
        self.order_map = {}
        self._orders = []
        self._closed_pairs = []
        self._last_entry = None
        self._restore(**kwargs)

    @property
    def active_pairs(self):
        return {idx: pair for idx, pair in self.pairs.items() if pair[0].is_closed != pair[1].is_closed}

    @property
    def total_value(self):
        return sum(o.value for o in self.active.values() if o.is_open)

    def create_solo(self, size: float, price: float,  side: str):
        order = Order(size, price, side, self.symbol)
        order.create(self.exchange)
        self.register_order(order)
        self.log_solo(order)

    def create_pair(self, order_pair: tuple, op_id: str, size: float | tuple) -> None:
        entry = self._create_entry(order_pair, size)
        exit = self._create_exit(order_pair, size)
        if self._submit_pair(entry, exit):
            self.register_pair(entry, exit, op_id)

    def register_pair(self, entry: Order, exit: Order, op_id: str):
        self.pairs[op_id] = (entry, exit)
        self.order_map[entry.id] = op_id
        self.order_map[exit.id] = op_id
        self.register_order(entry)
        self.register_order(exit)
        self.log_pair(op_id)

    def register_order(self, order: Order):
        if order.id not in self.active:
            self.active[order.id] = order
        self._orders.append(order.id)

    def remove_pair(self, op_id: str) -> tuple | None:
        if op_id in self.pairs:
            orders = self.pairs.pop(op_id)
            for o in orders:
                self.remove_order(o.id)
            return orders

    def remove_order(self, oid: str):
        if oid in self.active:
            del self.active[oid]
        if oid in self.order_map:
            del self.order_map[oid]

    def edit_pair(self, op_id: str, **kwargs):
        entry, exit = self.remove_pair(op_id)
        entry.edit(self.exchange, **kwargs)
        exit.edit(self.exchange, **kwargs)
        self.register_pair(entry, exit, op_id)

    def edit_order(self, oid: str, **kwargs):
        if oid in self.active:
            order = self.active[oid]
            status = order.edit(self.exchange, **kwargs)
            if status == "success" or status == "new":
                self.replace_oid(oid, order.id)
                self.register_order(order)
            self.log_edit(order, status, **kwargs)
            return order
        return None

    def replace_oid(self, old_oid: str, new_oid: str):
        self.order_map = utils.replace_key(self.order_map, old_oid, new_oid)
        self.active = utils.replace_key(self.active, old_oid, new_oid)

    def replace_pair(self, price_pair: tuple, op_id: str, size_pair: tuple) -> None:
        entry, exit = self.remove_pair(op_id)
        entry.cancel(self.exchange)
        exit.cancel(self.exchange)
        self.create_pair(price_pair, op_id, size_pair)

    def reset(self) -> None:
        pass

    def parse_closed(self, oid: str) -> None:
        if oid in self.active:
            self.active[oid].set_close()
            op_id = self.check_pair(oid)
            return op_id

    def close_pair(self, op_id: str):
        pass

    def check_pair(self, oid: str) -> None:
        op_id = self.order_map[oid] if oid in self.order_map else None
        if op_id is not None and op_id in self.pairs:
            if self.pairs[op_id][0].is_closed and self.pairs[op_id][1].is_closed:
                self._closed_pairs.append(op_id)
            if self.pairs[op_id][0].id == oid:
                self._last_entry = op_id
                return op_id

    @logger.catch
    def _submit_pair(self, entry: Order, exit: Order) -> bool:
        is_entry = entry.create(self.exchange)
        if not is_entry:
            del entry, exit
            return False
        is_exit = exit.create(self.exchange)
        if not is_exit:
            utils.safe_cancel_order(self.exchange, entry.id)
            del entry, exit
            return False
        return True

    def _create_entry(self, order_pair, size: float | tuple):
        side = "buy" if order_pair[0] < order_pair[1] else "sell"
        size = size[0] if isinstance(size, tuple) else size
        price = order_pair[0]
        return Order(size, price, side, self.symbol)

    def _create_exit(self, order_pair, size: float | tuple):
        side = "sell" if order_pair[0] < order_pair[1] else "buy"
        size = size[1] if isinstance(size, tuple) else size
        price = order_pair[1]
        params = {"triggerPrice":order_pair[0]}
        return Order(size, price, side, self.symbol, params=params)