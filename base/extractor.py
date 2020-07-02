class Extractor:
    def __init__(self, name):
        self.name = name

    def extract(self, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self.extract(**kwargs)
