from playhouse.postgres_ext import PostgresqlExtDatabase
# refactor above code into a class
class Database:
    def __init__(self):
        self.db: PostgresqlExtDatabase = None

    def is_closed(self) -> bool:
        if self.db is None:
            return True
        return self.db.is_closed()

    def is_open(self) -> bool:
        if self.db is None:
            return False
        return not self.db.is_closed()

    def close_connection(self) -> None:
        if self.db:
            self.db.close()
            self.db = None

    def open_connection(self) -> None:
        # if it's not None, then we already have a connection
        if self.db is not None:
            return

        options = {
            "keepalives": 1,
            "keepalives_idle": 60,
            "keepalives_interval": 10,
            "keepalives_count": 5
        }

        self.db = PostgresqlExtDatabase(
            "postgres",
            user="postgres",
            password="43523452345!",
            host="localhost",
            port=int(5432),
            sslmode="disable",
            **options
        )

        # connect to the database
        self.db.connect()


database = Database()