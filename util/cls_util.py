from typing import List


class CodeUtil:
    @staticmethod
    def func_args(func) -> List[str]:
        args: List[str]
        args = list(func.__code__.co_varnames)
        if "self" in args:
            args.remove("self")
        return args
