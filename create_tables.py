import configparser
import psycopg2
from time import time
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    start = time()
    print('Start Droping the tables if exists')
    print('-' * 30)
    print()
    length = len(drop_table_queries)
    i = 1
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'Dropped table {str(i)} of {str(length)} tables')
        i += 1
    end = time()
    minutes, seconds = divmod(end - start, 60)
    minutes_ = f'{minutes:.0f} Minutes' if minutes > 0 else ''
    seconds_ = f'{seconds:.2f} Seconds' if seconds > 0 else ''
    and_ = ' and ' if (minutes > 0 and seconds > 0) else ''
    print(f'Finished Dropping the tables in {minutes_}{and_}{seconds_}')
    print('-' * 30)
    print()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    start = time()
    print('Start Creating the tables')
    print('-' * 30)
    print()
    length = len(create_table_queries)
    i = 1
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'Created table {str(i)} of {str(length)} tables')
        i += 1
    end = time()
    minutes, seconds = divmod(end - start, 60)
    minutes_ = f'{minutes:.0f} Minutes' if minutes > 0 else ''
    seconds_ = f'{seconds:.2f} Seconds' if seconds > 0 else ''
    and_ = ' and ' if (minutes > 0 and seconds > 0) else ''
    print(f'Finished Creatung the tables in {minutes_}{and_}{seconds_}')
    print('-' * 30)
    print()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()