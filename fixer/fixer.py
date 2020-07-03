class Fixer:
    def __init__(self, name):
        self.name = name
        pass

    def fix(self, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self.fix(**kwargs)
