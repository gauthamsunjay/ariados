import abc
import json
import os
import atexit


class Store(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def store(self, data):
        raise NotImplementedError


class JsonStore(Store):
    def __init__(self, store_dir="./data"):
        if not os.path.exists(store_dir):
            os.makedirs(store_dir)
        self.filename = os.path.join(store_dir, "results.json")
        self.fh = open(self.filename, "a", buffering=1024)
        atexit.register(self.fh.close)

    def store(self, data):
        json.dump(data, self.fh, indent=2)


class S3Store(Store):
    def __init__(self):
        pass

    def store(self, data):
        super(S3Store, self).store(data)
