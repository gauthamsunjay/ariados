import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


def run_forever(fn, interval=None):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        while True:
            try:
                fn(*args, **kwargs)
            except Exception as e:
                # TODO log this
                logger.error("Got exception %r", e, exc_info=True)

            if interval is not None:
                time.sleep(interval)

    return wrapper
