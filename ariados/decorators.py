import re

def pattern(regex):
    def deco(fn):
        compiled = re.compile(regex)
        if not hasattr(fn, 'patterns'):
            fn.patterns = []

        fn.patterns.append(compiled)
        return fn
    return deco
