class Transformer:
    def __init__(self, name):
        self.name = name

    def transform(self, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self.transform(**kwargs)

    @staticmethod
    def name():
        return "vanilla-transformer"
