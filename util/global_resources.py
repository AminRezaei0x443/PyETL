from util.singleton import Singleton


class GlobalResources(metaclass=Singleton):
    def __init__(self):
        self.data = {}
        pass

    def add(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data[key]

    def get_safe(self, key):
        if key in self.data:
            return self.data[key], True
        return None, False
