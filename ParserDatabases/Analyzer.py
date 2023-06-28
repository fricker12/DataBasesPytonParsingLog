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