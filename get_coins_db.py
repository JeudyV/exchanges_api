import pandas as pd
import ccxt
#import mysql
from sqlalchemy import create_engine
from credentials_db import user, passw, host, port, database


user = user
passw = passw
host = host
port = port
database = database

mydb = create_engine('mysql+pymysql://' + user + ':' + passw + '@' + host + ':' + str(port) + '/' + database, echo=False)

#array_exchanres_error = []

def get_coins_db():

    from exchanges_information import get_coin_exchange_with_params

    query1 = """
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'test_exchanges_coins' and table_name like '%_markets_list%'
    """

    query = """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{}';
    """.format(database)

    df = pd.read_sql_query(query, mydb)

    df_exchanges_list = df.values.tolist()

    #print(df_exchanges_list)

    for i in df_exchanges_list:

        df_exchanges_coins = pd.read_sql_table(i[0], mydb)

        df_exchanges_coins_list = df_exchanges_coins.values.tolist()

        for market in df_exchanges_coins_list[:1]:

          exchange_name = i[0].replace('_markets_list','')

          print(exchange_name)

          market_name = market[0]

          print(market_name)

          #print("EXCHANGE: {}, MARKET: {}".format(exchange_name, market_name))

          #print("-- GETTING DATA EXCHANGE: {}, MARKET: {}".format(exchange_name, market_name))

          get_coin_exchange_with_params(exchange_name, market_name)

          #print("-- FINISHING EXCHANGE: {}, MARKET: {}".format(exchange_name, market_name))



get_coins_db()

def get_coins_db_gdax():

    from exchanges_information import get_coin_exchange_with_params

    query2 = """
            SELECT * FROM coin_exchange_db.gdax_markets_list;
        """.format(database)

    df = pd.read_sql_query(query2, mydb)

    df_exchanges_list = df.values.tolist()

    print(df_exchanges_list)

    for coin in df_exchanges_list:

        #df_exchanges_coins = pd.read_sql_table(i[0], mydb)

        #df_exchanges_coins_list = df_exchanges_coins.values.tolist()

        print("coin list ", coin)

        for market in coin[:1]:

          exchange_name = 'gdax'

          print("exchange_name", exchange_name)

          market_name = market

          print("market_name", market_name)

          #print("EXCHANGE: {}, MARKET: {}".format(exchange_name, market_name))

          #print("-- GETTING DATA EXCHANGE: {}, MARKET: {}".format(exchange_name, market_name))

          get_coin_exchange_with_params(exchange_name, market_name)

          #print("-- FINISHING EXCHANGE: {}, MARKET: {}".format(exchange_name, market_name))



#get_coins_db_gdax()