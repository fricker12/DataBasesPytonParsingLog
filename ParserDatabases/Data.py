import time
import re
class LogDataManager:
    def __init__(self, db_connector, database_type):
        self.db_connector = db_connector
        self.database_type = database_type

    def import_log_data(self, log_file):
        self.db_connector.connect()
        cursor = self.db_connector.connection.cursor()
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
                values = (
                    ip_address, forwarded_for, timestamp, request, status_code, response_size, time_taken, referer,
                    user_agent, balancer_worker_name)
                cursor.execute(sql, values)

        self.db_connector.connection.commit()
        cursor.close()
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Импорт лога в базу данных выполнен за {execution_time} секунд.")

    def export_log_data(self, log_file):
        self.db_connector.connect()
        cursor = self.db_connector.connection.cursor()
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