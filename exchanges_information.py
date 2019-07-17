import ccxt
import pandas as pd
from sqlalchemy import create_engine
import logging
from credentials_db import user, passw, host, port, database


user = user
passw = passw
host = host
port = port
database = database

mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database, echo=False)


def coin_exchange_into_db():
    list = []
    for exchange_id in ccxt.exchanges:
        try:
            exchange_nema = exchange_id + '_markets_list'
            print("exchange1 ", exchange_nema)
            list.append(exchange_nema)
            exchange = getattr(ccxt, exchange_id)()
            print(exchange_id)
            df = pd.DataFrame(exchange.load_markets().keys(), columns=["coin_list"])
            df.to_sql(name=exchange_id + "_markets_list", con=mydb, if_exists='replace', index=False, index_label='id')
        except Exception as error:
            #print("exchange_id: {}, Error: {}".format(exchange_id, error))

            # Create a custom logger
            logger = logging.getLogger(__name__)

            # Create handlers
            f_handler = logging.FileHandler('file_name_exchange_get_coin_exchange.log')

            # Create formatters and add it to handlers
            f_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
            f_handler.setFormatter(f_format)

            # Add handlers to the logger
            logger.addHandler(f_handler)
            logger.error('EXCHANGE: %s', exchange_id)
            logging.error('EXCHANGE: %s', exchange_id)

            continue

    print(list)
    df = pd.DataFrame(list, columns=["name_exchanges"])
    df.to_sql(name="exchange", con=mydb, if_exists='replace', index=False, index_label='id')


#coin_exchange_into_db()


def get_coin_exchange_with_params(exchange, coin):

        try:

            print(exchange)

            exchange_ = getattr(ccxt, exchange)()

            print(exchange_)

            if exchange == 'gdax':

                from control_gdax_exchange import get_data_by_market_gdax

                dg = get_data_by_market_gdax(exchange, coin)

            elif exchange == 'poloniex':

                from control_poloniex_exchange import get_data_by_market_polonix

                exchange == 'poloniex'

                dp = get_data_by_market_polonix(exchange_, coin, exchange)

                print(exchange)

            elif exchange == 'hitbtc':

                from control_hitbtc_exchange import get_data_by_market_hitbtc

                exchange_ == 'hitbtc2'

                dh = get_data_by_market_hitbtc(exchange_, coin, exchange)

                print(exchange)

            else:
                from exchanges_control import get_data_by_market

                df = get_data_by_market(exchange_, coin, exchange)

        except Exception as error:

           #print("***exchange_id: {}, Error: {}".format(exchange, error))
           logging.error('%s ***exchange: %s ***coin %s ***error ', exchange, coin, error)
            
#get_coin_exchange()

