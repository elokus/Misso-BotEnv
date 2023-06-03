from threading import Lock, Thread
import TestMisso.services.utilities as utils
import ccxt

class SingletonMeta(type):
    """
    This is a thread-safe implementation of Singleton.
    """

    _instances = {}

    _lock: Lock = Lock()
    """
    We now have a lock object that will be used to synchronize threads during
    first access to the Singleton.
    """

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        # Now, imagine that the program has just been launched. Since there's no
        # Singleton instance yet, multiple threads can simultaneously pass the
        # previous conditional and reach this point almost at the same time. The
        # first of them will acquire lock and will proceed further, while the
        # rest will wait here.
        with cls._lock:
            # The first thread to acquire the lock, reaches this conditional,
            # goes inside and creates the Singleton instance. Once it leaves the
            # lock block, a thread that might have been waiting for the lock
            # release may then enter this section. But since the Singleton field
            # is already initialized, the thread won't create a new object.
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]

class Singleton(metaclass=SingletonMeta):
    exchange: ccxt = None
    """
    We'll use this property to prove that our Singleton really works.
    """

    def __init__(self) -> None:
        self.exchange = utils.initialize_exchange_driver("Main")

    def some_business_logic(self):
        """
        Finally, any singleton should define some business logic, which can be
        executed on its instance.
        """

def get_singleton():
    singleton = Singleton()
    print(singleton.exchange.headers)

def test_singleton(sub: str) -> None:
    singleton = Singleton()
    singleton.exchange.headers["FTX-SUBACCOUNT"] = sub
    print(singleton.exchange.headers)

if __name__ == "__main__":
    process1 = Thread(target=test_singleton, args=("testing_3",))
    process2 = Thread(target=get_singleton, args=())
    process1.start()
    process2.start()