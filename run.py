import argparse
import logging 
from ParserDatabases import Connector
from ParserDatabases import Analyzer
from ParserDatabases import Data

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

    db_connector = Connector.DatabaseConnector(
        args.database,
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        db_name=args.db_name
    )
    db_connector.connect()
    log_data_manager = Data.LogDataManager(db_connector, database_type=args.database)
    log_data_manager.import_log_data(args.log_file)
    log_analyzer = Analyzer.LogAnalyzer(db_connector)
    log_data_manager.export_log_data("exported_log_file.txt")
    
    ip_user_agent_stats = log_analyzer.get_ip_user_agent_statistics(5)
    print("IP Address\tUser Agent\tFrequency")
    for stats in ip_user_agent_stats:
        print(f"{stats[0]}\t{stats[1]}\t{stats[2]}")
        
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()

