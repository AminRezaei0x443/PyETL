from typing import Dict, List


class DictUtil:
    @staticmethod
    def filter(d: Dict, keep: List[str]) -> Dict:
        nd = {}
        for k in d:
            if k in keep:
                nd[k] = d[k]
        return nd
