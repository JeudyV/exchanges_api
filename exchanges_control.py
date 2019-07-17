import pandas as pd
import ccxt
from sqlalchemy import create_engine
import datetime
from datetime import datetime as dt
import traceback
from pprint import pprint
import logging
import pymysql
from credentials_db import user, passw, host, port, database


user = user
passw = passw
host = host
port = port
database = database

mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database, echo=False)

epoch_now = datetime.datetime.now().timestamp()*1000

array_exchanres_error = []
dict = {}

def get_data_by_market(client, coin, exchange_name):
    start_point_time = '2000-11-08T00:00:00'

    actual = epoch_now

    #print("COIN: {}".format(coin))
    #print("EXCHANGE NAME: {}".format(exchange_name))

    listI = []

    #global array_exchanres_error

    global array_exchanres_error
    global dict

    start_point_time_ = client.parse8601(start_point_time)

    try:

        next_date_start = start_point_time_

        next_date_start = client.iso8601(start_point_time_)

        epoch_next_date_start = 0

        #print(start_point_time_)

        flag = True

        while epoch_next_date_start < actual and flag is True:

            #print(client.parse8601(next_date_start))

            ohlcv = client.fetch_ohlcv(coin, '1h', client.parse8601(next_date_start), 750)

            #print("-- Getting data COIN:{} for actual: {} start_date: {} next_formatted_date: {} elements: {} total_in_list {}".format(coin, actual, start_point_time_, next_date_start, len(ohlcv), len(listI)))

            listI = listI + ohlcv

            if next_date_start == client.iso8601(ohlcv[-1][0]):
                
                flag = False

            else:

                next_date_start = client.iso8601(ohlcv[-1][0])

            #print(next_date_start)

            epoch_next_date_start = ohlcv[-1][0]
            
            #print("Getting data COIN:{} for start_date: {} next_formatted_date: {} elements: {} total_in_list {}".format(coin, start_point_time_, next_date_start, len(ohlcv), len(listI)))
            #print("I: {}".format(ohlcv))



        df = pd.DataFrame(listI, columns=["timestamp", "open", "high", "low", "close", "volume"])

        df.to_sql(name='{}_{}'.format(exchange_name, coin), con=mydb, if_exists='replace', index=False, index_label='id')

    except Exception as error:

        # Create a custom logger
        logger = logging.getLogger(__name__)

        #array_exchanres_error.append(exchange_name)
        #dict.update({exchange_name: error})
        #print(dict)
        #print(array_exchanres_error)

        #print("EXCHANGE: {}, ERROR: {}".format(exchange_name, error))
        #logging.error('%s raised an error', exchange_name)

        # Create handlers
        f_handler = logging.FileHandler('data_by_market_error.log')

        # Create formatters and add it to handlers
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(f_handler)
        logger.error('EXCHANGE: %s ERROR %s', exchange_name, error)
        logging.error('EXCHANGE: %s', exchange_name)

        #print("COIN: {}, Error: {}".format(coin, error))

        #print(traceback.format_exc())

        pass

    """if len(array_exchanres_error) != 1:
        array_exchanres_error = list(set(array_exchanres_error))
        #print(array_exchanres_error)

    df = pd.DataFrame.from_dict(dict, orient="index")
    print(df)
    df.to_csv('exchanges_file_error.csv')"""


    #df = pd.DataFrame(array_exchanres_error, columns=['exchange_name'])
    #df.to_csv('exchanges_file_error.csv')

#get_data()


