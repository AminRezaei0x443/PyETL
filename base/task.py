class Task:
    def __init__(self):
        pass

    def do(self, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self.do(**kwargs)
