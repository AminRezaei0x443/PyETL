from typing import Dict


class SqlLoader:
    commands: Dict[str, str] = dict()

    def __init__(self, path):
        self.path = path
        self._load()

    def _load(self):
        with open(self.path, 'rb') as f:
            d = f.read().decode('utf-8')
            cur = ""
            for line in d.split("\n"):
                if line.startswith("--!"):
                    cur = line.replace("--!", "").replace(":", "").strip()
                    self.commands[cur] = ""
                elif line.startswith("--"):
                    pass
                else:
                    if cur in self.commands:
                        self.commands[cur] += line + "\n"

    def pick(self, key, **kwargs):
        d = self.commands[key]
        for k in kwargs:
            d = d.replace("${%s}" % k, str(kwargs[k]))
        return d
