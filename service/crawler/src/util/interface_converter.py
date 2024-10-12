from abc import ABC, abstractmethod

class IConverter(ABC):
    @abstractmethod
    def convert(self):
        pass
