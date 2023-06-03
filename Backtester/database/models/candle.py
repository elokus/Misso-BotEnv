import peewee
from Backtester.database.clients.postgres import database

# Model for postgres peewee ORM

if database.is_closed():
    database.open_connection()

class Candle(peewee.Model):
    id = peewee.UUIDField(primary_key=True)
    timestamp = peewee.BigIntegerField()
    open = peewee.FloatField()
    high = peewee.FloatField()
    low = peewee.FloatField()
    close = peewee.FloatField()
    volume = peewee.FloatField()
    exchange = peewee.CharField()
    symbol = peewee.CharField()
    timeframe = peewee.CharField()

    # partial candles: 5 * 1m candle = 5m candle while 1m == partial candle
    is_partial = True

    class Meta:
        from Backtester.database.clients.postgres import database

        database = database.db
        indexes = (
            (('exchange', 'symbol', 'timeframe', 'timestamp'), True),
        )

    def __init__(self, attributes: dict = None, **kwargs) -> None:
        peewee.Model.__init__(self, attributes=attributes, **kwargs)

        if attributes is None:
            attributes = {}

        for a, value in attributes.items():
            setattr(self, a, value)


# if database is open, create the table
if database.is_open():
    Candle.create_table()