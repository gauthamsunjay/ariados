import abc


class Status(object):
    WAITING = "waiting"
    PROCESSING = "processing"
    COMPLETED = "completed"


class Database(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, db_name):
        self.db_name = db_name

    @abc.abstractmethod
    def insert_link(self, url, ):
        raise NotImplementedError

    @abc.abstractmethod
    def insert_links(self, urls):
        raise NotImplementedError

    @abc.abstractmethod
    def get_link(self, status=Status.WAITING):
        raise NotImplementedError

    @abc.abstractmethod
    def get_links(self, status=Status.WAITING, num_links=-1):
        raise NotImplementedError

    @abc.abstractmethod
    def update_link(self, url, status):
        raise NotImplementedError

    @abc.abstractmethod
    def update_links(self, urls, status):
        raise NotImplementedError

    @abc.abstractmethod
    def delete_link(self, url):
        raise NotImplementedError

    @abc.abstractmethod
    def delete_links(self, urls):
        raise NotImplementedError
