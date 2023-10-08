from abc import ABC, abstractmethod
from pygrib import gribmessage


class GribMessage(ABC):
    @property
    @abstractmethod
    def min_lat(self):
        pass

    @property
    @abstractmethod
    def max_lat(self):
        pass

    @property
    @abstractmethod
    def min_lon(self):
        pass

    @property
    @abstractmethod
    def max_lon(self):
        pass

    @property
    @abstractmethod
    def step_lan(self):
        pass

    @property
    @abstractmethod
    def step_lon(self):
        pass

    @abstractmethod
    def values(self):
        pass

class GribMessagePyGrib(GribMessage):
    def __init__(self, message: gribmessage):
        self.message = message
        lats, lons = message.latlons()
        self.lats = lats
        self.lons = lons

    @property
    def min_lat(self):
        return self.lats.min()

    @property
    def max_lat(self):
        return self.lats.max()

    @property
    def min_lon(self):
        return self.lons.min()

    @property
    def max_lon(self):
        return self.lons.max()

    @property
    def step_lan(self):
        return self.message.iDirectionIncrementInDegrees

    @property
    def step_lon(self):
        return self.message.jDirectionIncrementInDegrees

    def values(self):
        # Разворачивает двухмерные values в одномерный массив,
        # причем развёртка идёт слева-направо, снизу-вверх
        for val in self.message.values.flatten():
            yield val
        return
