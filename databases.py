import argparse
import pymysql
import psycopg2
import sqlite3
import h2
from pymongo import MongoClient
import redis
import re

def connect_mysql(host, port, username, password, database):
    connection = pymysql.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        charset='utf8'
    )

    # Создание базы данных, если она не существует
    create_db_query = f"CREATE DATABASE IF NOT EXISTS {database}"
    with connection.cursor() as cursor:
        cursor.execute(create_db_query)

    # Подключение к созданной или существующей базе данных
    connection = pymysql.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        database=database,
        charset='utf8'
    )

    return connection

def connect_postgresql(host, port, username, password, database):
    connection = psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        dbname=database
    )
    return connection

def connect_sqlite(database):
    connection = sqlite3.connect(database)
    return connection

def connect_h2():
    connection = h2.H2Database(":memory:")
    return connection

def connect_mongodb(host, port, username, password, database):
    client = MongoClient(host, port, username=username, password=password)
    connection = client[database]
    return connection

def connect_redis(host, port, password, database):
    connection = redis.Redis(host=host, port=port, password=password, db=database)
    return connection

def import_log_data(connection, log_file):
    cursor = connection.cursor()

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

    connection.commit()
    cursor.close()

def export_log_data(connection):
    # Ваш код экспорта данных из базы данных в лог-файл
    cursor = connection.cursor()

    # Получение данных из базы данных
    select_query = "SELECT * FROM log_data"
    cursor.execute(select_query)
    data = cursor.fetchall()

    with open('export_log', 'w') as file:
        for row in data:
            # Преобразование данных в формат журнала лога
            log_entry = f"{row[1]} ({row[2]}) - - [{row[3]}] \"{row[4]}\" {row[5]} {row[6]} {row[7]} {row[10]} \"{row[9]}\" \"{row[8]}\"\n"
            file.write(log_entry)

    cursor.close()
    
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    return result

def get_ip_user_agent_statistics(connection, n):
    query = f"""
        SELECT ip_address, user_agent, COUNT(*) as count
        FROM log_data
        GROUP BY ip_address, user_agent
        ORDER BY count DESC
        LIMIT {n}
    """
    result = execute_query(connection, query)
    return result

def get_query_frequency(connection, dT):
    cursor = connection.cursor()

    sql = """
    SELECT DATE_FORMAT(timestamp, '%%Y-%%m-%%d %%H:%%i') AS interval_start, COUNT(*) AS frequency
    FROM log_data
    GROUP BY interval_start
    ORDER BY interval_start
    """
    cursor.execute(sql)

    result = cursor.fetchall()
    cursor.close()

    return result

def get_top_user_agents(connection, N):
    cursor = connection.cursor()

    sql = """
    SELECT user_agent, COUNT(*) AS frequency
    FROM log_data
    GROUP BY user_agent
    ORDER BY frequency DESC
    LIMIT %s
    """
    cursor.execute(sql, (N,))

    result = cursor.fetchall()
    cursor.close()

    return result

def get_status_code_statistics(connection, dT):
    cursor = connection.cursor()

    sql = """
    SELECT status_code, COUNT(*) AS frequency
    FROM log_data
    WHERE status_code LIKE '5%'
        AND timestamp >= NOW() - INTERVAL %s MINUTE
    GROUP BY status_code
    """
    cursor.execute(sql, (dT,))

    result = cursor.fetchall()
    cursor.close()

    return result

def get_longest_shortest_requests(connection, N, longest=True):
    cursor = connection.cursor()

    order_by = "DESC" if longest else "ASC"

    sql = f"""
    SELECT request, length(request) AS request_length
    FROM log_data
    ORDER BY request_length {order_by}
    LIMIT %s
    """
    cursor.execute(sql, (N,))

    result = cursor.fetchall()
    cursor.close()

    return result

def get_common_requests(connection, N, slash_count):
    cursor = connection.cursor()

    sql = f"""
    SELECT SUBSTRING_INDEX(request, ' ', {slash_count+1}) AS request_pattern, COUNT(*) AS frequency
    FROM log_data
    WHERE request LIKE 'GET %'
    GROUP BY request_pattern
    ORDER BY frequency DESC
    LIMIT %s
    """
    cursor.execute(sql, (N,))

    result = cursor.fetchall()
    cursor.close()

    return result

def get_upstream_requests_WORKER(connection):
    cursor = connection.cursor()

    sql = """
    SELECT BALANCER_WORKER_NAME, COUNT(*) AS request_count, AVG(TIME) AS average_time
    FROM log_data
    WHERE BALANCER_WORKER_NAME IS NOT NULL
    GROUP BY BALANCER_WORKER_NAME
    """
    cursor.execute(sql)

    result = cursor.fetchall()
    cursor.close()

    return result

def get_conversion_statistics(connection, sort_by):
    cursor = connection.cursor()

    sql = """
    SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(Referer, '/', 3), '/', -1) AS domain,
           COUNT(*) AS conversion_count
    FROM log_data
    WHERE Referer IS NOT NULL
    GROUP BY domain
    ORDER BY {} DESC
    """.format(sort_by)

    cursor.execute(sql)

    result = cursor.fetchall()
    cursor.close()

    return result

def get_upstream_requests(connection, interval):
    cursor = connection.cursor()

    sql = """
    SELECT DATE_FORMAT(`time`, '%%Y-%%m-%%d %%H:%%i:%%s') AS timestamp,
           COUNT(*) AS upstream_request_count
    FROM log_data
    WHERE `time` >= DATE_SUB(NOW(), INTERVAL %s)
          AND `time` <= NOW()
          AND `BALANCER_WORKER_NAME` IS NOT NULL
    GROUP BY timestamp
    ORDER BY timestamp
    """

    cursor.execute(sql, (interval,))

    result = cursor.fetchall()
    cursor.close()

    return result

def find_most_active_periods(connection, N):
    cursor = connection.cursor()

    sql = """
    SELECT CONCAT(DATE_FORMAT(`time`, '%%Y-%%m-%%d %%H:'), LPAD((MINUTE(`time`) DIV %s) * %s, 2, '0')) AS period,
           COUNT(*) AS request_count
    FROM log_data
    WHERE `time` >= DATE_SUB(NOW(), INTERVAL %s)
    GROUP BY period
    ORDER BY request_count DESC
    LIMIT %s
    """

    cursor.execute(sql, (N, N, N, N))

    result = cursor.fetchall()
    cursor.close()

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database Connection and Log Data Handling")
    parser.add_argument("database", choices=["mysql", "postgresql", "sqlite", "h2", "mongodb", "redis"], help="Database type")
    parser.add_argument("--host", help="Database host")
    parser.add_argument("--port", type=int, help="Database port")
    parser.add_argument("--username", help="Database username")
    parser.add_argument("--password", help="Database password")
    parser.add_argument("--db-name", help="Database name")
    parser.add_argument("--log-file", help="Path to log file")
    
    subparsers = parser.add_subparsers(dest="command")
    ip_user_agent_parser = subparsers.add_parser("ip_user_agent_statistics", help="Get IP User Agent statistics")
    ip_user_agent_parser.add_argument("--n", type=int, help="Number of top records to display")
    
    query_frequency_parser = subparsers.add_parser("query_frequency", help="Get query frequency")
    query_frequency_parser.add_argument("--dT", type=int, help="Time interval in minutes")
    
    top_user_agents_parser = subparsers.add_parser("top_user_agents", help="Get top User Agents")
    top_user_agents_parser.add_argument("--N", type=int, help="Number of top User Agents to display")
    
    status_code_parser = subparsers.add_parser("status_code_statistics", help="Get status code statistics")
    status_code_parser.add_argument("--dT", type=int, help="Time interval in minutes")
    
    longest_requests_parser = subparsers.add_parser("longest_requests", help="Get longest requests")
    longest_requests_parser.add_argument("--N", type=int, help="Number of longest requests to display")
    shortest_requests_parser = subparsers.add_parser("shortest_requests", help="Get shortest requests")
    shortest_requests_parser.add_argument("--N", type=int, help="Number of shortest requests to display")
    
    common_requests_parser = subparsers.add_parser("common_requests", help="Get common requests")
    common_requests_parser.add_argument("--N", type=int, help="Number of common requests to display")
    common_requests_parser.add_argument("--slash_count", type=int, help="Number of slashes to analyze")
    
    upstream_requests_WORKER = subparsers.add_parser("upstream_requests_WORKER", help="Get upstream requests")
    
    conversion_statistics_parser = subparsers.add_parser("conversion_statistics", help="Get conversion statistics")
    conversion_statistics_parser.add_argument("--sort_by", choices=["domain", "conversion_count"], default="domain",
                                              help="Field to sort the results by")
    
    upstream_requests_parser = subparsers.add_parser("upstream_requests", help="Get upstream requests")
    upstream_requests_parser.add_argument("--dT", type=str, help="Time interval for upstream requests")
    
    most_active_periods_parser = subparsers.add_parser("most_active_periods", help="Get most active periods")
    most_active_periods_parser.add_argument("--N", type=int, help="Number of most active periods to display")


    
    args = parser.parse_args()

    connection = None

    if args.database == "mysql":
        connection = connect_mysql(args.host, args.port, args.username, args.password, args.db_name)
    elif args.database == "postgresql":
        connection = connect_postgresql(args.host, args.port, args.username, args.password, args.db_name)
    elif args.database == "sqlite":
        connection = connect_sqlite(args.db_name)
    elif args.database == "h2":
        connection = connect_h2()
    elif args.database == "mongodb":
        connection = connect_mongodb(args.host, args.port, args.username, args.password, args.db_name)
    elif args.database == "redis":
        connection = connect_redis(args.host, args.port, args.password, args.db_name)

    if connection is not None:
        if args.log_file:
            import_log_data(connection, args.log_file)
            export_log_data(connection)
        elif args.command == "ip_user_agent_statistics":
            n = args.n
            result = get_ip_user_agent_statistics(connection, n)
            for row in result:
                print(row)
        elif args.command == "query_frequency":
            dT = args.dT
            result = get_query_frequency(connection, dT)
            for row in result:
                print(row)
        elif args.command == "top_user_agents":
            N = args.N
            result = get_top_user_agents(connection, N)
            for row in result:
                print(row)
        elif args.command == "status_code_statistics":
            dT = args.dT
            result = get_status_code_statistics(connection, dT)
            for row in result:
                print(row)
        elif args.command == "longest_requests":
            N = args.N
            result = get_longest_shortest_requests(connection, N, longest=True)
            for row in result:
                print(row)
        elif args.command == "shortest_requests":
            N = args.N
            result = get_longest_shortest_requests(connection, N, longest=False)
            for row in result:
                print(row)
        elif args.command == "common_requests":
            N = args.N
            slash_count = args.slash_count
            result = get_common_requests(connection, N, slash_count)
            for row in result:
                print(row)
        elif args.command == "upstream_requests_WORKER":
            result = get_upstream_requests_WORKER(connection)
            for row in result:
                print(row)
        if args.command == "conversion_statistics":
            result = get_conversion_statistics(connection, args.sort_by)
            for row in result:
                print(row)
        if args.command == "upstream_requests":
            result = get_upstream_requests(connection, args.dT)
            for row in result:
                print(row)
        if args.command == "most_active_periods":
            result = find_most_active_periods(connection, args.N)
            for row in result:
                print(row)
        else:
            query = input("Enter your database query: ")
            execute_query(connection, query)
    else:
        print("Invalid database type. Please choose one of: mysql, postgresql, sqlite, h2, mongodb, redis.")
