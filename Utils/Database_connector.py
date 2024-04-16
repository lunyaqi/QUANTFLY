import csv
import io

import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from config.conf import POSTGRES_CONFIG
from functools import wraps
import psycopg2.extras


def singleton(cls):
    instances = {}

    @wraps(cls)
    def get_instance(*args, **kw):
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]

    return get_instance


def psql_insert_copy(table, conn, keys, data_iter):  # mehod
    """
    Execute SQL statement inserting data

    Parameters
    ----------
    table : pandas.io.sql.SQLTable
    conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    keys : list of str
        Column names
    data_iter : Iterable that iterates the values to be inserted
    """
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


def insert_df_to_postgres(df: pd.DataFrame, table_name: str):
    client = PostgresClient()
    df.to_sql(table_name, con=client.engine, index=False, if_exists='append')


@singleton
class PostgresClient(object):
    """PostgreSQL client singleton"""
    default_conn = None

    def __init__(self, username=None, password=None, database_name=None):

        self.username = username if username is not None else POSTGRES_CONFIG["user"]
        self.password = password if password is not None else POSTGRES_CONFIG["password"]
        self.database_name = database_name if database_name is not None else POSTGRES_CONFIG["database"]
        self.host = POSTGRES_CONFIG["host"]
        self.port = POSTGRES_CONFIG["port"]
        self.engine = self.__create_engine()

    def __create_engine(self):
        try:
            connection_string = f'postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}'
            engine = create_engine(connection_string)
            return engine
        except Exception as e:
            print('Failed to connect to the database: %s' % e)
            raise

    def get_result(self, sql_expr):
        with self.engine.connect() as conn:
            result = conn.execute(sql_expr).fetchall()
            return result

    def read_sql(self, sql_expr):
        df = pd.read_sql(sql_expr, self.engine)
        return df

    def insert_dataframe(self, table_name, df, method='append'):
        try:
            df.to_sql(table_name, self.engine, if_exists=method, index=False, method=psql_insert_copy)
        except Exception as e:
            print(e)

# @singleton
# class PostgresClient(object):
#     """PostgreSQL client singleton"""
#     default_conn = None
#
#     def __init__(self, username=None, password=None, database_name=None):
#         self.username = username if username is not None else POSTGRES_CONFIG["user"]
#         self.password = password if password is not None else POSTGRES_CONFIG["password"]
#         self.database = database_name if database_name is not None else POSTGRES_CONFIG["database"]
#         self.conn = self.__get_conn()
#     def __get_conn(self):
#         default_connect_params = {
#             'host': POSTGRES_CONFIG['host'],
#             'port': POSTGRES_CONFIG['port'],
#             'user': self.username,
#             'password': self.password,
#             'database': self.database
#         }
#
#         try:
#             conn = psycopg2.connect(**default_connect_params)
#             return conn
#         except Exception as e:
#             print('Failed to connect to the database: %s' % e)
#             raise
#
#     def get_result(self, sql_expr):
#         cursor = self.conn.cursor()
#         try:
#             cursor.execute(sql_expr)
#             result = cursor.fetchall()
#             return result
#         finally:
#             cursor.close()
#             self.conn.close()
#
#     def read_sql(self, sql_expr):
#         conn = self.__get_conn()
#         try:
#             df = pd.read_sql(sql_expr, conn)
#             return df
#         finally:
#             conn.close()
#
#     def insert_dataframe(self,table_name, df):
#         conn = self.__get_conn()
#         if len(df) > 0:
#             df_columns = list(df)
#             columns = ",".join(df_columns)
#             values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
#             insert_stmt = "INSERT INTO {} ({}) {}".format(table_name, columns, values)
#             curs = conn.cursor()
#             psycopg2.extras.execute_batch(curs, insert_stmt, df.values)
#             conn.commit()
#             curs.close()
