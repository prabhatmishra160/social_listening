from brightfield_common.settings.conf import settings
import psycopg2

kwargs = {
    "database": settings.POSTGRESQL_DB, "user": settings.POSTGRESQL_USER,
    "host": settings.POSTGRESQL_HOST, "port": settings.POSTGRESQL_PORT,
    "password": settings.POSTGRESQL_PASSWORD
}


class Postgres:

    def __init__(self):
        self.connection = psycopg2.connect(**kwargs)
        self.cursor = self.connection.cursor()

    def execute(self, table, columns):
        self.cursor.execute(f"SELECT {columns} FROM  public.{table};")

    def fetch_all(self, table, columns='*'):
        self.execute(table, columns)
        return self.cursor.fetchall()

    def query(self, q):
        self.cursor.execute(q)
        return self.cursor.fetchall()
