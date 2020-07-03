class Loader:
    def __init__(self, name):
        self.name = name

    def load(self, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self.load(**kwargs)

    @staticmethod
    def name():
        return "vanilla-loader"
