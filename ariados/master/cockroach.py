import functools
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from db import Database, Status


Base = declarative_base()


def safe_commit(fn):
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            fn(self, *args, **kwargs)
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
    return wrapper


class Link(Base):
    __tablename__ = "links"

    id = Column(Integer, primary_key=True, autoincrement=True)
    link = Column(String, nullable=False, unique=True)
    status = Column(String, default=Status.WAITING)

    def to_dict(self):
        return {"link": self.link, "status": self.status}


class Cockroach(Database):
    def __init__(self, db_name, user="ariados", host="localhost", port="26257"):
        super(Cockroach, self).__init__(db_name)
        # connect in insecure mode
        engine = create_engine(
            "cockroachdb://%s@%s:%s/%s" % (user, host, port, db_name),
            connect_args={"sslmode": "disable"}
        )
        Session = sessionmaker(bind=engine)

        if not engine.has_table("links"):
            Base.metadata.create_all(engine)
        self.session = Session()

    @safe_commit
    def insert_link(self, url, status=Status.WAITING):
        instance = self.session.query(Link).filter_by(link=url).first()
        if not instance:
            self.session.add(Link(link=url, status=status))

    @safe_commit
    def insert_links(self, urls, status=Status.WAITING):
        instances = []
        for url in urls:
            instance = self.session.query(Link).filter_by(link=url).first()
            if not instance:
                instances.append(Link(link=url, status=status))
        self.session.add_all(instances)

    def get_link(self, status=Status.WAITING):
        query = self.session.query(Link)
        link = query.filter_by(status=status).first()
        if link is not None:
            link = link.to_dict()
        return link

    def get_links(self, status=Status.WAITING, num_links=-1):
        query = self.session.query(Link).filter_by(status=status)
        if num_links != -1:
            query = query.limit(num_links)

        for link in query:
            yield link.to_dict()

    @safe_commit
    def update_link(self, url, status):
        self.session.query(Link).filter_by(link=url).update(
            {"status": status}, synchronize_session="fetch"
        )

    @safe_commit
    def update_links(self, urls, status):
        self.session.query(Link).filter(Link.link.in_(urls)).update(
            {"status": status}, synchronize_session="fetch"
        )

    @safe_commit
    def delete_link(self, url):
        self.session.query(Link).filter_by(link=url).delete(
            synchronize_session="fetch"
        )

    @safe_commit
    def delete_links(self, urls):
        self.session.query(Link).filter(Link.link.in_(urls)).delete(
            synchronize_session="fetch"
        )
