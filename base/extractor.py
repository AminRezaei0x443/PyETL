class Extractor:

    def __init__(self, name):
        self.name = name

    def extract(self, **kwargs):
        pass

    def __call__(self, **kwargs):
        return self.extract(**kwargs)

    @staticmethod
    def name():
        return "vanilla-extractor"
