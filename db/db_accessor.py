class DatabaseAccessor:

    def __init__(self):
        pass

    def connect(self):
        pass

    def close(self):
        pass

    def cursor(self, reuse=True):
        pass

    def exec(self, query: str, reuse_cursor=True):
        pass

    def fetch_all(self):
        pass

    def commit(self):
        pass
