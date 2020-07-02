from base.task import Task
from base.transformer import Transformer


class Transform(Task):
    def __init__(self):
        super().__init__()
        self.stack = []
        self.transformers = {}
        pass

    def define(self, transformer: Transformer):
        self.transformers[transformer.name] = transformer

    def add(self, transformer, **transformer_params):
        if transformer not in self.transformers:
            raise RuntimeError("Data Extractor %s has not been defined! Available Ones: %s" %
                               (transformer, ",".join([e for e in self.transformers])))
        self.stack.append((self.transformers[transformer], transformer_params))

    def do(self, **kwargs):
        res = {}
        for s in self.stack:
            transformer, params = s
            res[transformer.name] = transformer(**params)
        return res

