from base.extract import Extract
from base.load import Load
from base.transform import Transform


class ETLPipeline:
    def __init__(self, **kwargs):
        self.args = kwargs
        self.structure_extract = Extract()
        self.extract = Extract()
        self.transform = Transform()
        self.structure_load = Load()
        self.load = Load()
        pass

    def def_extractor(self, extractor):
        self.extract.define(extractor)

    def def_transformer(self, transformer):
        self.transform.define(transformer)

    def def_loader(self, loader):
        self.load.define(loader)

    def def_structure_extractor(self, extractor):
        self.structure_extract.define(extractor)

    def add_structure_extractor(self, extractor_name, **args):
        if len(self.structure_extract) == 1:
            print("Just one structure extraction and inferring is possible")
        self.structure_extract.add(extractor_name, **args)

    def def_structure_loader(self, loader):
        self.structure_load.define(loader)

    def add_structure_loader(self, loader_name, **args):
        if len(self.structure_load) == 1:
            print("Just one structure loading and inferring is possible")
        self.structure_load.add(loader_name, **args)

    def add_extractor(self, extractor_name, **args):
        self.extract.add(extractor_name, **args)

    def add_transformer(self, transformer_name, **args):
        self.transform.add(transformer_name, **args)

    def add_loader(self, loader_name, **args):
        self.load.add(loader_name, **args)

    def run(self, **kwargs):
        pass

    def __call__(self, **kwargs):
        return self.run(**kwargs)
