import abc
import json
import os


class Store(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def store(self, key, value):
        raise NotImplementedError


class JsonStore(Store):
    def __init__(self, store_dir="./data"):
        self.store_dir = store_dir
        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir)

    def store(self, key, value):
        with open(os.path.join(self.store_dir, "%s.json" % key), "w") as fp:
            json.dump(fp, value, indent=2)


class S3Store(Store):
    def __init__(self):
        pass

    def store(self, key, value):
        super(S3Store, self).store(key, value)
