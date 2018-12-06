import abc
import json
import os
import atexit

from ariados.common import stats


class Store(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def store(self, data):
        raise NotImplementedError


class JsonStore(Store):
    def __init__(self, store_dir="./data"):
        # NOTE since this is running on the master, we just want to simulate storing of data
        # not really storing the data now as it eats up disk space on our laptops.
        return

        if not os.path.exists(store_dir):
            os.makedirs(store_dir)
        self.filename = os.path.join(store_dir, "results.json")
        self.fh = open(self.filename, "a", buffering=1024)
        atexit.register(self.fh.close)

    def store(self, data):
        serialized_data = "%s\n" % json.dumps(data)
        stats.client.incr("store.writes", count=len(serialized_data.encode("utf-8")))
        # NOTE not proceeding with the write since this runs embedded in master
        return

        self.fh.write(serialized_data)



class S3Store(Store):
    def __init__(self):
        pass

    def store(self, data):
        super(S3Store, self).store(data)
