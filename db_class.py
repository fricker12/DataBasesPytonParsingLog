import argparse
import pymysql
import psycopg2
import sqlite3
import h2
from pymongo import MongoClient
import redis
import re
import time

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

    def import_log_data(self, log_file):
        cursor = self.connection.cursor()
        start_time = time.time()

        # Создание таблицы log_data, если она не существует
        create_table_query = """
            CREATE TABLE IF NOT EXISTS log_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ip_address VARCHAR(255),
                forwarded_for VARCHAR(255),
                timestamp VARCHAR(255),
                request LONGTEXT,
                status_code INT,
                response_size INT,
                time_taken INT,
                referer LONGTEXT,
                user_agent LONGTEXT,
                balancer_worker_name VARCHAR(255)
            )
        """
        cursor.execute(create_table_query)

        with open(log_file, 'r') as file:
            log_data = file.readlines()

        for line in log_data:
            regex = r'^(?P<ip_address>\S+) \((?P<forwarded_for>\S+)\) - - \[(?P<timestamp>[\w:/]+\s[+\-]\d{4})\] "(?P<request>[A-Z]+ \S+ \S+)" (?P<status_code>\d+) (?P<response_size>\d+) (?P<time_taken>\d+) (?P<balancer_worker_name>\d+) "(?P<Referer>[^"]*)" "(?P<user_agent>[^"]*)"'
            match = re.match(regex, line)
            if match:
                ip_address = match.group('ip_address')
                forwarded_for = match.group('forwarded_for')
                timestamp = match.group('timestamp')
                request = match.group('request')
                status_code = match.group('status_code')
                response_size = match.group('response_size')
                time_taken = match.group('time_taken')
                balancer_worker_name = match.group('balancer_worker_name')
                referer = match.group('Referer')
                user_agent = match.group('user_agent')

                sql = "INSERT INTO log_data (ip_address, forwarded_for, timestamp, request, status_code, response_size, time_taken, referer, user_agent, balancer_worker_name) " \
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                values = (ip_address, forwarded_for, timestamp, request, status_code, response_size, time_taken, referer, user_agent, balancer_worker_name)
                cursor.execute(sql, values)

        self.connection.commit()
        cursor.close()
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Импорт лога в базу данных выполнен за {execution_time} секунд.")
    # Создание экспорт данных лога из базы в файл
    def export_log_data(self, log_file):
        cursor = self.connection.cursor()
        start_time = time.time()
        select_query = "SELECT * FROM log_data"
        cursor.execute(select_query)
        log_data = cursor.fetchall()

        with open(log_file, 'w') as file:
            for data in log_data:
                line = f"{data[1]} ({data[2]}) - - [{data[3]}] \"{data[4]}\" {data[5]} {data[6]} {data[7]} {data[10]} \"{data[8]}\" \"{data[9]}\"\n"
                file.write(line)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Экспорт лога из базы данных выполнен за {execution_time} секунд.")
    # Запрос к базе данных
    def execute_query(self, query,params=None):
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        result = cursor.fetchall()
        return result
    
class LogAnalyzer:
    def __init__(self, db_connector):
        self.db_connector = db_connector
    
    def get_ip_user_agent_statistics(self, n):
        query = f"""
            SELECT ip_address, user_agent, COUNT(*) as count
            FROM log_data
            GROUP BY ip_address, user_agent
            ORDER BY count DESC
            LIMIT {n}
        """
        result = self.db_connector.execute_query(query)
        return result
    
    def get_query_frequency(self, dT):
        query = """
            SELECT DATE_FORMAT(timestamp, '%%Y-%%m-%%d %%H:%%i') AS interval_start, COUNT(*) AS frequency
            FROM log_data
            GROUP BY interval_start
            ORDER BY interval_start
        """
        result = self.db_connector.execute_query(query)
        return result
    
    def get_top_user_agents(self, N):
        query = """
            SELECT user_agent, COUNT(*) AS frequency
            FROM log_data
            GROUP BY user_agent
            ORDER BY frequency DESC
            LIMIT %s
        """
        result = self.db_connector.execute_query(query, (N,))
        return result
    
    def get_status_code_statistics(self, dT):
        query = """
            SELECT status_code, COUNT(*) AS frequency
            FROM log_data
            WHERE status_code LIKE '5%%'
            AND timestamp >= NOW() - INTERVAL %s MINUTE
            GROUP BY status_code
        """
        result = self.db_connector.execute_query(query, (dT,))
        return result
    
    def get_longest_shortest_requests(self, limit, order_by):
        if order_by == "longest":
            order_by_clause = "DESC"
        elif order_by == "shortest":
            order_by_clause = "ASC"
        else:
            raise ValueError("Invalid order_by value. Must be 'longest' or 'shortest'.")

        query = f"""
            SELECT request, time_taken
            FROM log_data
            ORDER BY time_taken {order_by_clause}
            LIMIT %s
        """
        result = self.db_connector.execute_query(query, (limit,))
        return result

    def get_common_requests(self, N, slash_count):
        query = f"""
            SELECT SUBSTRING_INDEX(request, ' ', {slash_count+1}) AS request_pattern, COUNT(*) AS frequency
            FROM log_data
            WHERE request LIKE 'GET %%'
            GROUP BY request_pattern
            ORDER BY frequency DESC
            LIMIT %s
        """
        result = self.db_connector.execute_query(query, (N,))
        return result
    
    def get_upstream_requests_WORKER(self):
        query = """
            SELECT BALANCER_WORKER_NAME, COUNT(*) AS request_count, AVG(timestamp) AS average_time
            FROM log_data
            WHERE BALANCER_WORKER_NAME IS NOT NULL
            GROUP BY BALANCER_WORKER_NAME
        """
        result = self.db_connector.execute_query(query)
        return result
    
    def get_conversion_statistics(self, sort_by):
        query = """
            SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(Referer, '/', 3), '/', -1) AS domain,
            COUNT(*) AS conversion_count
            FROM log_data
            WHERE Referer IS NOT NULL
            GROUP BY domain
            ORDER BY {} DESC
        """.format(sort_by)

        result = self.db_connector.execute_query(query)
        return result
    
    def get_upstream_requests(self, interval):
        query = """
            SELECT COUNT(*) AS upstream_request_count, AVG(time_taken) AS average_time
            FROM log_data
            WHERE `timestamp` >= NOW() - INTERVAL %s
                AND `BALANCER_WORKER_NAME` IS NOT NULL
        """
        result = self.db_connector.execute_query(query, (interval,))
        return result
    
    def find_most_active_periods(self, N):
        query = """
            SELECT CONCAT(DATE_FORMAT(`timestamp`, '%%Y-%%m-%%d %%H:'), LPAD((MINUTE(`timestamp`) DIV %s) * %s, 2, '0')) AS period,
            COUNT(*) AS request_count
            FROM log_data
            WHERE `timestamp` >= DATE_SUB(NOW(), INTERVAL %s)
            GROUP BY period
            ORDER BY request_count DESC
            LIMIT %s
        """
        result = self.db_connector.execute_query(query, (N, N, N, N))
        return result
    
def main():
    parser = argparse.ArgumentParser(description="Log Analyzer")
    parser.add_argument("--database", type=str, required=True, choices=["mysql", "postgresql", "sqlite", "h2", "mongodb", "redis"], help="Database type")
    parser.add_argument("--host", type=str, required=True, help="Database host")
    parser.add_argument("--port", type=int, required=True, help="Database port")
    parser.add_argument("--username", type=str, required=True, help="Database username")
    parser.add_argument("--password", type=str, required=True, help="Database password")
    parser.add_argument("--db_name", type=str, required=True, help="Database name")
    parser.add_argument("--log_file", type=str, required=True, help="Path to the log file")
    args = parser.parse_args()

    db_connector = DatabaseConnector(
        args.database,
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        db_name=args.db_name
    )
    db_connector.connect()
    db_connector.import_log_data(args.log_file)
    log_analyzer = LogAnalyzer(db_connector)
    
    ip_user_agent_stats = log_analyzer.get_ip_user_agent_statistics(5)
    print("IP Address\tUser Agent\tFrequency")
    for stats in ip_user_agent_stats:
        print(f"{stats[0]}\t{stats[1]}\t{stats[2]}")
            
    query_frequency = log_analyzer.get_query_frequency(60)
    print("Interval Start\tFrequency")
    for freq in query_frequency:
        print(f"{freq[0]}\t{freq[1]}")

    top_user_agents = log_analyzer.get_top_user_agents(10)
    print("User Agent\tFrequency")
    for agent in top_user_agents:
        print(f"{agent[0]}\t{agent[1]}")

    status_code_stats = log_analyzer.get_status_code_statistics(60)
    print("Status Code\tFrequency")
    for code in status_code_stats:
        print(f"{code[0]}\t{code[1]}")

    longest_requests = log_analyzer.get_longest_shortest_requests(5, "longest")
    print("Longest Requests")
    for request in longest_requests:
        print(f"Request: {request[0]}\tTime Taken: {request[1]}")

    shortest_requests = log_analyzer.get_longest_shortest_requests(5, "shortest")
    print("Shortest Requests")
    for request in shortest_requests:
        print(f"Request: {request[0]}\tTime Taken: {request[1]}")

    common_requests = log_analyzer.get_common_requests(5, 2)
    print("Request Pattern\tFrequency")
    for request in common_requests:
        print(f"{request[0]}\t{request[1]}")

    upstream_requests = log_analyzer.get_upstream_requests_WORKER()
    print("Balancer Worker\tRequest Count\tAverage Time")
    for request in upstream_requests:
        print(f"{request[0]}\t{request[1]}\t{request[2]}")

    conversion_stats = log_analyzer.get_conversion_statistics("conversion_count")
    print("Domain\tConversion Count")
    for stats in conversion_stats:
        print(f"{stats[0]}\t{stats[1]}")

    upstream_requests = log_analyzer.get_upstream_requests('30 SECOND')
    print("Upstream Request Count\tAverage Time")
    for request in upstream_requests:
        print(f"{request[0]}\t{request[1]}")

    active_periods = log_analyzer.find_most_active_periods(5)
    print("Period\tRequest Count")
    for period in active_periods:
        print(f"{period[0]}\t{period[1]}")

    db_connector.export_log_data("exported_log.txt")

if __name__ == "__main__":
    main()
