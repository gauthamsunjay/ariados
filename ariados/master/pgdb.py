import hashlib

import psycopg2
from psycopg2.extras import execute_values

class Status(object):
    WAITING = "waiting"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class PostgresDB(object):
    def __init__(self, db_name):
        self.conn = psycopg2.connect("dbname=%s user=ariados host=localhost port=5432" % db_name)

        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS links (id CHAR(32) PRIMARY KEY, link TEXT, status VARCHAR(32) DEFAULT '%s');
        """.strip() % Status.WAITING)

        self.conn.commit()
        cur.close()

    def get_links(self, num_links, status=Status.WAITING):
        cursor = self.conn.cursor()
        cursor.execute("""SELECT link from links WHERE status=%s LIMIT %s""", (status, num_links))
        result = cursor.fetchall()
        cursor.close()
        return [ i[0] for i in result ]

    def get_links_to_be_crawled(self, num_links):
        query = """
        WITH cte AS (
            select id, link from links where status=%s limit %s
        )
        UPDATE links l SET status=%s FROM cte WHERE cte.id = l.id RETURNING cte.link
        """
        cursor = self.conn.cursor()
        cursor.execute(query, (Status.WAITING, num_links, Status.PROCESSING))
        result = cursor.fetchall()
        self.conn.commit()
        cursor.close()

        return [i[0] for i in result]

    def insert_batch(self, batch):
        cursor = self.conn.cursor()
        batch_with_ids = [
            (hashlib.md5(link).hexdigest(), link) for link in batch
        ]

        execute_values(cursor, """INSERT INTO links (id, link) VALUES %s ON CONFLICT(id) DO NOTHING""", batch_with_ids)
        self.conn.commit()
        cursor.close()

    def update_batch(self, batch):
        cursor = self.conn.cursor()
        batch_with_ids = [
            (hashlib.md5(link).hexdigest(), status) for link, status in batch
        ]

        execute_values(cursor, """UPDATE links SET status = data.status FROM (VALUES %s) AS data (id, status) WHERE links.id = data.id""", batch_with_ids)
        # execute_values(cursor, """UPDATE links(id, link, status) VALUES %S ON CONFLICT(id) DO UPDATE SET status=EXCLUDED.status""", batch_with_ids)
        self.conn.commit()
        cursor.close()

    def get_status_count(self):
        cursor = self.conn.cursor()
        cursor.execute("""SELECT status, count(link) from links GROUP BY status""")
        result = cursor.fetchall()
        cursor.close()

        status_dict = { getattr(Status, i): 0 for i in dir(Status) if i.isupper() }
        status_dict.update(dict(result))
        return status_dict
