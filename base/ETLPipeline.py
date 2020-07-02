from base.extract import Extract
from base.load import Load
from base.transform import Transform


class ETLPipeline:
    def __init__(self, **kwargs):
        self.args = kwargs
        self.extract = Extract()
        self.transform = Transform()
        self.load = Load()
        pass



