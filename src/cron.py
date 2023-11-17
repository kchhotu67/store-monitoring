import schedule
import time
import pandas as pd
import sqlite3
import pytz


def create_store_status_table(conn):
    cursor = conn.cursor()
    print("create store_status table if does not exist...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS store_status (
            store_id TEXT,
            timestamp_utc INTEGER,
            status TEXT,
            UNIQUE(store_id, timestamp_utc)
        )
    ''')
    # Create an index on the store_id column
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_store_id ON store_status(store_id)')
    conn.commit()


def create_timezone_table(conn):
    cursor = conn.cursor()
    print("create store_timezone table if does not exist...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS store_timezone (
            store_id TEXT,
            timezone_str TEXT,
            timezone_offset REAL,
            PRIMARY KEY (store_id)
        )
    ''')
    conn.commit()

def create_menu_hours_table(conn):
    cursor = conn.cursor()
    print("create menu_hours table if does not exist...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS menu_hours (
            store_id TEXT,
            day INTEGER,
            start_time_local TEXT,
            end_time_local TEXT,
            UNIQUE(store_id, day, start_time_local, end_time_local)
        )
    ''')
    # Create an index on the store_id column
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_store_id ON menu_hours(store_id)')
    conn.commit()

def insert_menu_hours_data_into_menu_hours_table(conn):
    cursor = conn.cursor()
    print("Inserting menu hours data into menu_hours table...")
    menu_hours_file = 'data/menu_hours.csv'
    df = pd.read_csv(menu_hours_file)
    for _, row in df.iterrows():
        try:
            cursor.execute(
                'INSERT INTO menu_hours VALUES (?, ?, ?, ?)', 
                (row['store_id'], row['day'], row['start_time_local'], row['end_time_local'])
            )
            conn.commit()
        except sqlite3.IntegrityError as e:
            # print(f"Row {row['store_id']}  {row['day']} {row['start_time_local']} {row['end_time_local']} not inserted due to unique constraint violation.")
            pass

def insert_timezone_data_into_timezone_table(conn):
    cursor = conn.cursor()
    print("Inserting store timezone data into store_timezone table...")
    timezone_file = 'data/timezones.csv'
    df = pd.read_csv(timezone_file)
    df['timezone_str'] = df['timezone_str'].fillna('America/Chicago')
    df['timezone_offset'] = df['timezone_str'].apply(lambda tz: int(pytz.timezone(tz).utcoffset(pd.Timestamp.now()).total_seconds()))
    for _, row in df.iterrows():
        try:
            cursor.execute('INSERT INTO store_timezone VALUES (?, ?, ?)', (row['store_id'], row['timezone_str'], row['timezone_offset']))
            conn.commit()
        except sqlite3.IntegrityError as e:
            # print(f"Error: {e}")
            # print(f"Row {row['store_id']} {row['timezone_str']} not inserted due to unique constraint violation.")
            pass


def insert_store_status_into_store_status_table(conn):
    cursor = conn.cursor()
    print("Inserting store status data into store_status table...")
    status_file = 'data/store_status.csv'
    df = pd.read_csv(status_file)
    df = df.head(100)
    df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'], format='mixed')
    df['timestamp_utc'] = df['timestamp_utc'].astype(int) // 10**9
    for _, row in df.iterrows():
        try:
            cursor.execute('INSERT INTO store_status VALUES (?, ?, ?)', (row['store_id'], row['timestamp_utc'], row['status']))
            conn.commit()
        except sqlite3.IntegrityError as e:
            # print(f"Row {row['store_id']}__{row['timestamp_utc']} not inserted due to unique constraint violation.")
            pass


def pull_data_every_hour():
    print('Pulling Data from datasource...')
    # Connect to SQLite database
    conn = sqlite3.connect('data/database.db')
    
    # Pull data for menu hours
    create_menu_hours_table(conn)
    insert_menu_hours_data_into_menu_hours_table(conn)

    # Pull data for store timezone
    create_timezone_table(conn)
    insert_timezone_data_into_timezone_table(conn)

    # Pull data for store status from data source
    create_store_status_table(conn)
    insert_store_status_into_store_status_table(conn)

    # close connection
    conn.commit()
    conn.close()

if __name__ == '__main__':
    print("Cron Service is Running...")
    schedule.every(1).hours.do(pull_data_every_hour)
    # Run the scheduled jobs in an infinite loop
    while True:
        schedule.run_pending()
        time.sleep(1)  # Sleep for 1 second to avoid high CPU usage

