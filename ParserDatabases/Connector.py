import pymysql
import psycopg2
import sqlite3
import h2
from pymongo import MongoClient
import redis

class DatabaseConnector:
    def __init__(self, database, host=None, port=None, username=None, password=None, db_name=None):
        self.database = database
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.db_name = db_name
        self.connection = None

    def connect(self):
        if self.database == "mysql":
            self.connection = self.connect_mysql()
        elif self.database == "postgresql":
            self.connection = self.connect_postgresql()
        elif self.database == "sqlite":
            self.connection = self.connect_sqlite()
        elif self.database == "h2":
            self.connection = self.connect_h2()
        elif self.database == "mongodb":
            self.connection = self.connect_mongodb()
        elif self.database == "redis":
            self.connection = self.connect_redis()

    def connect_mysql(self):
        connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            charset='utf8'
        )

        # Создание базы данных, если она не существует
        create_db_query = f"CREATE DATABASE IF NOT EXISTS {self.db_name}"
        with connection.cursor() as cursor:
            cursor.execute(create_db_query)

        # Подключение к созданной или существующей базе данных
        connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            database=self.db_name,
            charset='utf8'
        )

        return connection

    def connect_postgresql(self):
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.username,
            password=self.password,
            dbname=self.db_name
        )
        return connection

    def connect_sqlite(self):
        connection = sqlite3.connect(self.db_name)
        return connection

    def connect_h2(self):
        connection = h2.H2Database(":memory:")
        return connection

    def connect_mongodb(self):
        client = MongoClient(self.host, self.port, username=self.username, password=self.password)
        connection = client[self.db_name]
        return connection

    def connect_redis(self):
        connection = redis.Redis(host=self.host, port=self.port, password=self.password, db=self.db_name)
        return connection

    # Запрос к базе данных
    def execute_query(self, query,params=None):
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()
        return result