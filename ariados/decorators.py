import re

from ariados.common import Handler

def handler(regex):
    # TODO maybe use classes here to maintain state.
    def deco(fn):
        # TODO accept re flags
        compiled = re.compile(regex)
        if not hasattr(fn, 'handler'):
            fn.handler = Handler(fn)

        fn.handler.patterns.append(compiled)
        return fn
    return deco
