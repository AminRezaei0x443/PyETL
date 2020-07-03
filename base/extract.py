from base.extractor import Extractor
from base.task import Task


class Extract(Task):
    def __init__(self):
        super().__init__()
        self.stack = []
        self.extractors = {}
        pass

    def define(self, extractor: Extractor):
        self.extractors[extractor.name] = extractor

    def add(self, extractor, **extractor_params):
        if extractor not in self.extractors:
            raise RuntimeError("Data Extractor %s has not been defined! Available Ones: %s" %
                               (extractor, ",".join([e for e in self.extractors])))
        self.stack.append((self.extractors[extractor], extractor_params))

    def do(self, **kwargs):
        res = {}
        for s in self.stack:
            extractor, params = s
            res[extractor.name] = extractor(**params)
        self.stack.clear()
        return res

    def __len__(self):
        return len(self.stack)
