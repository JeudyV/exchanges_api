from poloniex import Poloniex
import pandas as pd
from time import time
import ccxt
import logging
import datetime
from sqlalchemy import create_engine
from credentials_db import user, passw, host, port, database

user = user
passw = passw
host = host
port = port
database = database

mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database,
                     echo=False)

epoch_now = datetime.datetime.now().timestamp() * 1
print(epoch_now)


def get_data_by_market_polonix(exchange, market_name):
    market_name = market_name.split(sep='/')
    market_name = '_'.join(market_name)
    api = Poloniex(jsonNums=float)
    start_point_time = 1554076800
    end_point_time = start_point_time + 2592000
    print(end_point_time)
    actual = epoch_now
    listI = []
    try:
        epoch_next_date_start = 0
        flag = True
        f = True
        while epoch_next_date_start < actual and flag is True:
            ListCoin = api.returnChartData(market_name, period=1800, start=start_point_time, end=end_point_time)
            listI = listI + ListCoin
            df = pd.DataFrame(ListCoin, columns=["date", "open", "high", "low", "close", "volume"])
            print(df)
            listI = listI + ListCoin
            print(listI)
            start_point_time = end_point_time
            print(start_point_time)
            end_point_time = end_point_time + 2592000
            print(end_point_time)
            if end_point_time >= actual:
                flag = False
            else:
                epoch_next_date_start = end_point_time
                print(epoch_next_date_start)

        df = pd.DataFrame(ListCoin, columns=["date", "open", "high", "low", "close", "volume"])
        df.to_sql(name='{}_{}'.format(exchange, market_name), con=mydb, if_exists='replace', index=False,
                  index_label='id')
        print(listI)
    except Exception as error:

        # Create a custom logger
        logger = logging.getLogger(__name__)

        # Create handlers
        f_handler = logging.FileHandler('gdax_exchanges.log')

        # Create formatters and add it to handlers
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(f_handler)
        logger.error('EXCHANGE: %s ERROR %s', exchange, error)
        logging.error('COIN: %s', market_name)

        pass


get_data_by_market_polonix('Poloniex', 'BTC/LTC')