import logging, queue
from Misso.models.position import Position
from Misso.models.order.order import Order

# BaseClass for SubManager

class MessageHandler:
    def __init__(self, subaccount: str,
                 in_msg_queue: queue.Queue,
                 out_msg_queue: queue.Queue,
                 logger: logging.Logger,
                 **kwargs):
        self.sub = subaccount
        self.name = f"{subaccount}_Messaging"
        self.in_msg = in_msg_queue
        self.out_msg = out_msg_queue
        self.logger = logger


    def init_configs(self, config):
        for key, value in config.items():
            setattr(self, key, value)

        # init Moduls

    def message_processing(self):
        while not self.in_msg.empty():
            msg = self.in_msg.get()
            self.mlog(f"has new watcher Message: {msg}")
            self.message_allocation(msg)

    def message_allocation(self, msg):
        # subaccount, issue, symbol, payload
        if msg[1] == "STOP":
            self.stop_routine()

        elif msg[1] == "CLEAN":
            self.cleaning_routine()

        elif msg[1] == "low_breakout":
            self.range_break_high(msg)

        elif msg[1] == "high_breakout":
            self.range_break_low(msg)

        elif msg[1] == "order_closed":
            self.order_closing(msg)

        elif msg[1] == "intervention":
            self.intervention_handler(msg)

        elif msg[1] == "out_of_risk":
            self.remove_risk_position(msg)


    ######
    ###  MODULS

    def cleaning_routine(self):
        pass

    def stop_routine(self):
        pass

    def range_break_low(self, msg):
        # sub, "low_breakout", market, watch_job
        pass

    def range_break_high(self, msg):
        # sub, "high_breakout", market, watch_job
        pass

    def order_closing(self, msg):
        # sub, "order_closed", market, (id, order)
        pass

    def remove_risk_position(self, msg):
        pass

    def intervention_handler(self, msg):
        pass

    def send_message(self, msg):
        self.mlog(f"putting msg: {msg}")
        self.out_msg.put(msg)


    def delegate_risk_position(self, pos: Position):
        msg = ("SafeGuard", {self.sub:pos, "risk_config":{"max_order_value":self.MAX_VALUE, "risk_value": self.RISK_VALUE}})
        self.send_message(msg)

    def remove_from_watcher(self, pos: Position):
        msg = ("watcher", ("remove_mw", self.sub, pos.symbol))
        self.send_message(msg)

    def add_to_watcher(self, pos: Position):
        msg = ("watcher", ("add_mw", self.sub, (pos.symbol, {"tx":pos.curr_tx, "range":pos.curr_range, "side":pos.slot})))
        self.send_message(msg)
        while len(pos._orders) > 0:
            oid = pos._orders.pop()
            self.watch_order_id(oid)


    def watch_order(self, order: Order):
        if order.id is not None:
            msg = ("watcher", ("add_order_id", self.sub, order.id))
            self.send_message(msg)
            
    def watch_order_id(self, order_id: str):
        if order_id is not None:
            msg = ("watcher", ("add_order_id", self.sub, order_id))
            self.send_message(msg)

    def watch_restored_order(self, order: Order):
        if order.id is not None:
            msg = ("watcher", ("add_init_order_id", self.sub, order.id))
            self.send_message(msg)

    def mlog(self, msg):
        self.logger.info(f"{self.name}: {msg}")