import sqlite3
import pandas as pd
import time
import datetime
import json
import multiprocessing


def determine_crossover_type(ema_low, ema_high, last_decision, threshold=0.1):
        if ema_low is not None and ema_high is not None:
            if ema_high + threshold < ema_low:
                return 1
            elif ema_high + threshold > ema_low:
                return 0
            elif last_decision == 1:
                return 1
            elif last_decision == 0:
                return 0
            else:
                return None
        else:
            return None

def fetch_crossover_data(db_path, threshold):
    last_decision = None
    try:
        while True:
            db = sqlite3.connect(db_path)
            c = db.cursor()
            c.execute("SELECT last_second_timestamp, instrument, close_price, ema_low, ema_high FROM min_data ORDER BY timestamp DESC LIMIT 1")
            crossover_data = c.fetchall()
            print(crossover_data[0])
            
            query = "INSERT INTO kotak_final_data (last_second_timestamp, instrument, close_price, ema_low, ema_high) VALUES (?, ?, ?, ?, ?)"
            c.execute(query, (crossover_data[0][0], crossover_data[0][1], crossover_data[0][2], crossover_data[0][3], crossover_data[0][4]))
            db.commit()

            query = "INSERT INTO icici_final_data (last_second_timestamp, instrument, close_price, ema_low, ema_high) VALUES (?, ?, ?, ?, ?)"
            c.execute(query, (crossover_data[0][0], crossover_data[0][1], crossover_data[0][2], crossover_data[0][3], crossover_data[0][4]))
            db.commit()
            
            crossover_decision = determine_crossover_type(crossover_data[0][3], crossover_data[0][4], last_decision, threshold=threshold)

            if last_decision == crossover_decision:
                Crossover = ""
                Description = 'No action required'
            else:
                if crossover_decision == 1:
                    Crossover = "BUY"
                    Description = "buy here"
                    
                    
                elif crossover_decision == 0:
                    Crossover = "SQUARE UP"
                    Description = "sell here"
            last_decision  = crossover_decision
            query = "INSERT INTO research_data (last_second_timestamp, instrument, close_price, ema_low, ema_high, crossover, description) VALUES (?, ?, ?, ?, ?, ?, ?)"
            c.execute(query, (crossover_data[0][0], crossover_data[0][1], crossover_data[0][2], crossover_data[0][3], crossover_data[0][4], Crossover, Description))
            db.commit()
                
            
            # Adjust the sleep duration based on your requirements
            time.sleep(1)  # Sleep for 5 seconds before fetching data again

    except Exception as e:
        print(f"Error: {e}")
    
    
def call_a_fun(creds):
    tradingsymbol = creds.get("tradingsymbol")
    exchange = creds.get("exchange")
    threshold = creds.get("threshold", 0.1)
    db_path = f'{exchange}{tradingsymbol}_LIVE_trading_kite_data_{str(datetime.date.today())}.db'
    fetch_crossover_data(db_path, threshold)

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