from base.loader import Loader
from base.task import Task
from base.transformer import Transformer


class Load(Task):
    def __init__(self):
        super().__init__()
        self.stack = []
        self.loaders = {}
        pass

    def define(self, loader: Loader):
        self.loaders[loader.name] = loader

    def add(self, loader, **loader_params):
        if loader not in self.loaders:
            raise RuntimeError("Data Extractor %s has not been defined! Available Ones: %s" %
                               (loader, ",".join([e for e in self.loaders])))
        self.stack.append((self.loaders[loader], loader_params))

    def do(self, **kwargs):
        res = {}
        for s in self.stack:
            loader, params = s
            res[loader.name] = loader(**params)
        return res

