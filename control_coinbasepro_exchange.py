import cbpro
import ccxt
import logging
import time
import datetime
import pandas as pd
from sqlalchemy import create_engine
from credentials_db import user, passw, host, port, database

user = user
passw = passw
host = host
port = port
database = database

mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database, echo=False)

epoch_now = datetime.datetime.now().timestamp()*1000

def get_data_by_market_coinbasepro(exchange, market_name):
    market_name = market_name.split(sep='/')
    market_name = '-'.join(market_name)
    coinbasepro = cbpro.PublicClient()
    client = ccxt.gdax
    start_point_time = '2019-04-08T00:00:00'
    actual = epoch_now
    listI = []
    start_point_time_epock = client.parse8601(start_point_time)
    end_point_time = start_point_time_epock + 864000000
    try:
        next_date_start_t = client.iso8601(start_point_time_epock)
        end_point_time_ = client.iso8601(end_point_time)
        epoch_next_date_start = 0
        flag = True
        while epoch_next_date_start < actual and flag is True:
            time.sleep(1)
            hist = coinbasepro.get_product_historic_rates(
                market_name,
                start=next_date_start_t,
                end=end_point_time_,
                granularity=3600)
            listI = listI + hist
            next_date_start_t = end_point_time_
            end_point_time_ = client.parse8601(end_point_time_)
            end_point_time_ = end_point_time_ + 864000000
            epoch_next_date_start = end_point_time_
            if end_point_time_ >= actual:
                flag = False
            else:
                end_point_time_ = client.iso8601(end_point_time_)
        df = pd.DataFrame(listI, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df.to_sql(name='{}_{}'.format(exchange, market_name), con=mydb, if_exists='replace', index=False,
                  index_label='id')
        print(listI)
    except Exception as error:

        # Create a custom logger
        logger = logging.getLogger(__name__)

        # Create handlers
        f_handler = logging.FileHandler('coinbasepro_exchanges.log')

        # Create formatters and add it to handlers
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(f_handler)
        logger.error('EXCHANGE: %s ERROR %s', exchange, error)
        logging.error('COIN: %s', market_name)

        pass

