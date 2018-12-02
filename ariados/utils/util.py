import time
import traceback
from functools import wraps

def run_forever(fn, interval=None):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        while True:
            try:
                fn(*args, **kwargs)
            except Exception as e:
                # TODO log this
                print "Got exception %r" % e
                print traceback.format_exc()

            if interval is not None:
                time.sleep(interval)

    return wrapper
