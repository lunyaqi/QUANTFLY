import psycopg2
import pandas as pd
from config.conf import POSTGRES_CONFIG
from functools import wraps

def singleton(cls):
    instances = {}

    @wraps(cls)
    def get_instance(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return get_instance

@singleton
class PostgresClient(object):
    """PostgreSQL client singleton"""
    default_conn = None

    def __init__(self, username=None, password=None, database_name=None):
        if database_name not in POSTGRES_CONFIG:
            raise ValueError(f"Invalid database_name: {database_name}")

        self.username = username if username is not None else POSTGRES_CONFIG["user"]
        self.password = password if password is not None else POSTGRES_CONFIG["password"]
        self.database_name = database_name

    def __get_conn(self):
        default_connect_params = {
            'host': POSTGRES_CONFIG['host'],
            'port': POSTGRES_CONFIG['port'],
            'user': self.username,
            'password': self.password,
            'database': self.database_name
        }

        try:
            conn = psycopg2.connect(**default_connect_params)
            return conn
        except Exception as e:
            print('Failed to connect to the database: %s' % e)
            raise

    def get_result(self, sql_expr):
        conn = self.__get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(sql_expr)
            result = cursor.fetchall()
            return result
        finally:
            cursor.close()
            conn.close()

    def read_sql(self, sql_expr):
        conn = self.__get_conn()
        try:
            df = pd.read_sql(sql_expr, conn)
            return df
        finally:
            conn.close()

# 使用示例
# postgres_client = PostgresClient(username='your_username', password='your_password', database_name='your_database')
# result = postgres_client.get_result('SELECT * FROM your_table')
# df = postgres_client.read_sql('SELECT * FROM your_table')
