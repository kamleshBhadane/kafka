import pandas as pd
import sqlite3
import time
import datetime
from kiteconnect import KiteConnect
import multiprocessing
import json
import time
from kafka import KafkaProducer
import confluent_producer


def create_sec_table(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sec_data 
                 (timestamp TEXT PRIMARY KEY, instrument TEXT, instrument_token INTEGER, last_price REAL)''')
    db.commit()


def insert_sec_data(db_path, timestamp, instrument, instrument_token, last_price):
    db = sqlite3.connect(db_path)
    c = db.cursor()



    query = "INSERT INTO sec_data (timestamp, instrument, instrument_token, last_price) VALUES (?, ?, ?, ?)"
    timestamp_str = str(timestamp)
    c.execute(query, (timestamp_str, instrument, instrument_token, last_price))
    db.commit()


def create_three_min_table(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS three_min_data 
                 (timestamp TEXT PRIMARY KEY, instrument TEXT, instrument_token INTEGER, 
                  average_price REAL, open_price REAL, high_price REAL, low_price REAL, 
                  close_price REAL, volume INTEGER, ema_low REAL, ema_high REAL, crossover TEXT, Description TEXT,
                  last_second_timestamp TEXT
              )''')
    db.commit()


def create_fifteen_min_table(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS fifteen_min_data 
                 (timestamp TEXT PRIMARY KEY, instrument TEXT, instrument_token INTEGER, 
                  average_price REAL, open_price REAL, high_price REAL, low_price REAL, 
                  close_price REAL, volume INTEGER, ema_low REAL, ema_high REAL, crossover TEXT, Description TEXT,
                  last_second_timestamp TEXT
              )''')
    db.commit()


def create_three_min_final_data(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS three_min_final_data 
                 (id INTEGER  PRIMARY KEY, last_second_timestamp TEXT, instrument TEXT, close_price REAL, ema_low REAL, ema_high REAL, crossover TEXT, description TEXT)''')
    db.commit()


def create_fifteen_min_final_data(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS fifteen_min_final_data 
                 (id INTEGER  PRIMARY KEY, last_second_timestamp TEXT, instrument TEXT, close_price REAL, ema_low REAL, ema_high REAL, crossover TEXT, description TEXT)''')
    db.commit()


def create_five_min_final_data(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS five_min_final_data 
                 (id INTEGER  PRIMARY KEY, last_second_timestamp TEXT, instrument TEXT, close_price REAL, ema_low REAL, ema_high REAL, crossover TEXT, description TEXT)''')
    db.commit()


def create_final_data(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS final_data 
                 (id INTEGER  PRIMARY KEY, last_second_timestamp TEXT, instrument TEXT, close_price REAL, ema_low REAL, ema_high REAL, crossover TEXT, description TEXT)''')
    db.commit()


def create_three_min_research_data(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS three_min_research_data 
                 (id INTEGER  PRIMARY KEY, last_second_timestamp TEXT, instrument TEXT, close_price REAL, ema_low REAL, ema_high REAL, crossover TEXT, description TEXT)''')
    db.commit()


def create_research_data(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS research_data 
                 (id INTEGER  PRIMARY KEY, last_second_timestamp TEXT, instrument TEXT, close_price REAL, ema_low REAL, ema_high REAL, crossover TEXT, description TEXT)''')
    db.commit()


def create_fifteen_min_research_data(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS fifteen_min_research_data 
                 (id INTEGER  PRIMARY KEY, last_second_timestamp TEXT, instrument TEXT, close_price REAL, ema_low REAL, ema_high REAL, crossover TEXT, description TEXT)''')
    db.commit()


def create_five_min_research_data(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS five_min_research_data 
                 (id INTEGER  PRIMARY KEY, last_second_timestamp TEXT, instrument TEXT, close_price REAL, ema_low REAL, ema_high REAL, crossover TEXT, description TEXT)''')
    db.commit()


def create_min_table(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS min_data 
                 (timestamp TEXT PRIMARY KEY, instrument TEXT, instrument_token INTEGER, 
                  average_price REAL, open_price REAL, high_price REAL, low_price REAL, 
                  close_price REAL, volume INTEGER, ema_low REAL, ema_high REAL, crossover TEXT, Description TEXT,
                  last_second_timestamp TEXT
              )''')
    db.commit()


def create_five_min_table(db_path):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS five_min_data 
                 (timestamp TEXT PRIMARY KEY, instrument TEXT, instrument_token INTEGER, 
                  average_price REAL, open_price REAL, high_price REAL, low_price REAL, 
                  close_price REAL, volume INTEGER, ema_low REAL, ema_high REAL, crossover TEXT, Description TEXT,
                  last_second_timestamp TEXT
              )''')
    db.commit()


def insert_min_data(db_path, timestamp, instrument, instrument_token, last_price, creds):
    db = sqlite3.connect(db_path)
    c = db.cursor()
    # Set the limit values

    ema_low = creds.get("ema_low")
    ema_high = creds.get("ema_high")
    minute_key = timestamp.replace(second=0, microsecond=0)
    timestamp_str = str(minute_key)
    print("minute time string is: ", timestamp_str)
    # minute time string is:  2023-12-14 14:05:00

    minute_data = {
        'open': None,
        'high': None,
        'low': None,
        'close': None,
        'volume': 0,
        'ema_low': None,
        'ema_high': None,
        'crossover': None,
        'Description': None,
        'last_second_timestamp': None
    }

    end_of_minute = timestamp + pd.Timedelta(minutes=1)
    c.execute("SELECT * FROM sec_data WHERE timestamp >= ? AND timestamp < ?", (timestamp_str, str(end_of_minute)))
    sec_data = c.fetchall()

    if sec_data:
        minute_data['open'] = sec_data[0][3]
        minute_data['close'] = sec_data[-1][3]
        minute_data['high'] = max(record[3] for record in sec_data)
        minute_data['low'] = min(record[3] for record in sec_data)
        minute_data['volume'] = len(sec_data)
        last_second_timestamp = sec_data[-1][0]
        minute_data['last_second_timestamp'] = last_second_timestamp

    # Fetch last 'ema_low' records for ema_low calculation
    c.execute(f"SELECT * FROM min_data ORDER BY timestamp DESC LIMIT {ema_low}")
    ema_low_data = c.fetchall()
    close_prices_ema_low = [record[7] for record in ema_low_data if record[7] is not None]
    if close_prices_ema_low:
        minute_data['ema_low'] = pd.Series(close_prices_ema_low).ewm(span=ema_low, adjust=False).mean().iloc[-1]

    # Fetch last 'ema_high' records for ema_high calculation
    c.execute(f"SELECT * FROM min_data ORDER BY timestamp DESC LIMIT {ema_high}")
    last_ema_high_data = c.fetchall()
    close_prices_ema_high = [record[7] for record in last_ema_high_data if record[7] is not None]
    if close_prices_ema_high:
        minute_data['ema_high'] = pd.Series(close_prices_ema_high).ewm(span=ema_high, adjust=False).mean().iloc[-1]

    c.execute("SELECT * FROM min_data WHERE timestamp=?", (timestamp_str,))
    existing_record = c.fetchone()

    if existing_record:
        query = "UPDATE min_data SET instrument=?, instrument_token=?, average_price=?, open_price=?, high_price=?, low_price=?, close_price=?, volume=?, ema_low=?, ema_high=?, last_second_timestamp=? WHERE timestamp=?"
        c.execute(query, (
            instrument, instrument_token, last_price, minute_data['open'], minute_data['high'],
            minute_data['low'], minute_data['close'], minute_data['volume'], minute_data['ema_low'],
            minute_data['ema_high'], minute_data['last_second_timestamp'], timestamp_str
        ))
    else:
        query = "INSERT INTO min_data (timestamp, instrument, instrument_token, average_price, open_price, high_price, low_price, close_price, volume, ema_low, ema_high, last_second_timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        c.execute(query, (
            timestamp_str, instrument, instrument_token, last_price,
            minute_data['open'], minute_data['high'], minute_data['low'],
            minute_data['close'], minute_data['volume'], minute_data['ema_low'], minute_data['ema_high'],
            minute_data['last_second_timestamp']
        ))

    db.commit()


def fetch_live_data(tradingsymbol, exchange, db_path, creds):
    last_decision = None
    main_instrument = f"{exchange}:{tradingsymbol}"
    while True:
        try:
            with open('creds.json', 'r') as f:
                credentials = json.load(f)

            api_key = credentials.get("API-KEY", "")
            access_token = credentials.get("Access-token", "")
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            live_data = kite.ltp([f"{exchange}:{tradingsymbol}"])
            print(live_data)
            # timestamp = pd.Timestamp.now()
            from datetime import datetime

            timestamp = datetime.now()

            for instrument, data in live_data.items():
                instrument_token = data['instrument_token']
                last_price = data['last_price']

                print(f"{timestamp} - {instrument}: {last_price}")

                x = {
                    "timestamp": str(timestamp),
                    "instrument": instrument,
                    "instrument_token": instrument_token,
                    "last_price": last_price
                }
                data = json.dumps(x)
                # Insert data into sec_data table
                confluent_producer.send_message_to_kafka('T_Sec_Data', str(instrument), str(data))
                confluent_producer.flush_producer()
                insert_sec_data(db_path, timestamp, instrument, instrument_token, last_price)

                # # Insert data into min_data table
                # insert_min_data(db_path, timestamp, main_instrument, instrument_token, last_price, creds)



        except Exception as e:
            print(f"Error: {e}")


def call_a_fun(creds):
    tradingsymbol = creds.get("tradingsymbol")
    exchange = creds.get("exchange")
    db_path = f'{exchange}{tradingsymbol}_LIVE_trading_kite_data_{str(datetime.date.today())}.db'

    create_sec_table(db_path)
    create_min_table(db_path)
    create_research_data(db_path)
    create_final_data(db_path)

    create_five_min_final_data(db_path)
    create_five_min_research_data(db_path)
    create_five_min_table(db_path)

    create_three_min_final_data(db_path)
    create_three_min_research_data(db_path)
    create_three_min_table(db_path)

    create_fifteen_min_final_data(db_path)
    create_fifteen_min_table(db_path)
    create_fifteen_min_research_data(db_path)

    fetch_live_data(tradingsymbol, exchange, db_path, creds)


def process_credentials(creds):
    print(creds)
    call_a_fun(creds)


if __name__ == "__main__":
    with open('creds.json', 'r') as f:
        credentials = json.load(f)
    trades = credentials.get("traders", [])

    # Create a multiprocessing pool
    pool = multiprocessing.Pool()

    # Use the pool to apply the function to each set of credentials
    pool.map(process_credentials, trades)

    # Close the pool to free up resources
    pool.close()
    pool.join()
