import cbpro
import datetime
import ccxt
import logging
from pprint import pprint
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

import gdax
import datetime

epoch_now = datetime.datetime.now().timestamp()*1000


def test2():
    publicClient = gdax.PublicClient()
    actual2 = datetime.datetime.now()
    client = ccxt.gdax
    print("actual2 ", actual2)
    actual = epoch_now
    ten_dias = datetime.timedelta(minutes=14400)
    print(ten_dias)
    print("actual", actual)
    flag = True
    now = '2018-11-18 00:00:00.000000'
    date_time_now = datetime.datetime.strptime(now, '%Y-%m-%d %H:%M:%S.%f')
    print("now ", now)
    date_time_str = '2018-11-08 00:00:00.000000'
    date_time_obj = datetime.datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S.%f')
    print("date_time_obj", date_time_obj)
    print("isoformat()", date_time_obj.isoformat())
    epoch_next_date_start = 0
    while epoch_next_date_start < actual and flag is True:
        hist = publicClient.get_product_historic_rates(
                    "BTC-EUR",
                    start=date_time_obj.isoformat(),
                    end=date_time_now.isoformat(),
                    granularity=3600)
        print(hist)
        if actual2.isoformat() <= client.iso8601(hist[-1][0]):
            print("en el if ", hist[-1][0])
            flag = False
        else:
            date_time_obj = date_time_obj + ten_dias
            print(date_time_obj)
            date_time_now = date_time_now + ten_dias
            print(date_time_now)
            print(date_time_now.isoformat())
            print(hist[-1][0])
            print(client.iso8601(hist[-1][0]))
            time.sleep(2)


def get_data_by_market_gdax(exchange, market_name):
    market_name = market_name.split(sep='/')
    market_name = '-'.join(market_name)
    publicClient = gdax.PublicClient()
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
            hist = publicClient.get_product_historic_rates(
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
        f_handler = logging.FileHandler('gdax_exchanges.log')

        # Create formatters and add it to handlers
        f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        f_handler.setFormatter(f_format)

        # Add handlers to the logger
        logger.addHandler(f_handler)
        logger.error('EXCHANGE: %s ERROR %s', exchange, error)
        logging.error('COIN: %s', market_name)

        pass


#get_data_by_market_gdax(exchange)

#test2()


def test2():
    product = "{}-{}".format("BTC", "EUR")
    publicClient = gdax.PublicClient()
    hist = publicClient.get_product_historic_rates(
                product,
                granularity=3600)
    pprint(hist)

#test2()

"""client = ccxt.bitmex
epoch_now = datetime.datetime.now().timestamp() * 1000
start_point_time = '2000-11-08T00:00:00'
actual = epoch_now
print(actual)
start_point_time_ = client.parse8601(start_point_time)
print(start_point_time_)
next_date_start = start_point_time_
next_date_start = client.iso8601(start_point_time_)
response = ccs.poloniex.public.returnChartData("BTC_LTC", start=960412893000, end=1559942493, period=1800)
print(response)"""


# coinbasepro equals gdax

"""public_client = cbpro.PublicClient()
epoch_now = datetime.datetime.now().timestamp()*1000
client = ccxt.coinbase

actual = epoch_now
actual = str(actual)
print(type(actual))
print(actual)
start_point_time = '2015-11-08T00:00:00'
start_point_time_ = client.parse8601(start_point_time)
start_point_time_ = str(start_point_time_)
print(type(start_point_time_))
print(start_point_time_)
print(public_client.get_products())

v = public_client.get_product_historic_rates('ETH-USD')
print("v", v)
# To include other parameters, see function docstring:
v1 = public_client.get_product_historic_rates('ETH-USD', start_point_time_, actual, 300)
print("v1", v1)"""

