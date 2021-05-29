import configparser
import psycopg2
from time import time
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    start = time()
    print('Start copying the data into staging_tables')
    print('-' * 30)
    print()
    length = len(copy_table_queries)
    i = 1
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'Copied data for {str(i)} of {str(length)} tables')
        i += 1
    end = time()
    minutes, seconds = divmod(end - start, 60)
    minutes_ = f'{minutes:.0f} Minutes' if minutes > 0 else ''
    seconds_ = f'{seconds:.2f} Seconds' if seconds > 0 else ''
    and_ = ' and ' if (minutes > 0 and seconds > 0) else ''
    print(f'Finished copying the data into staging_tables in {minutes_}{and_}{seconds_}')
    print('-' * 30)
    print()



def insert_tables(cur, conn):
    start = time()
    print('Start Inserting the data into tables')
    print('-' * 30)
    print()
    length = len(insert_table_queries)
    i = 1
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print(f'Inserted data for {str(i)} of {str(length)} tables')
        i += 1
    end = time()
    minutes, seconds = divmod(end - start, 60)
    minutes_ = f'{minutes:.0f} Minutes' if minutes > 0 else ''
    seconds_ = f'{seconds:.2f} Seconds' if seconds > 0 else ''
    and_ = ' and ' if (minutes > 0 and seconds > 0) else ''
    print(f'Finished Inserting the data into tables in {minutes_}{and_}{seconds_}')
    print('-' * 30)
    print()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()