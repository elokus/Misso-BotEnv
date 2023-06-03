from __future__ import annotations
from abc import ABC, abstractmethod

class DatabaseClient:

    @abstractmethod
    def is_closed(self) -> bool:
        pass

    @abstractmethod
    def is_open(self) -> bool:
        pass

    @abstractmethod
    def close_connection(self) -> None:
        pass

    @abstractmethod
    def open_connection(self) -> None:
        pass

class DatabaseInterface:
    @abstractmethod
    def store(self, *args, **kwargs):
        pass

    @abstractmethod
    def fetch(self, *args, **kwargs):
        pass

