class Position:
    def __init__(self, market: str, side: str):
        self.SYMBOL = market
        self.SIDE = side
        self.orders = OrderHandler()
