import creds
import dblib
import marketmanager as MM
import datetime
import requests
import logging
from pyhomebroker import HomeBroker
import pyhomebroker.common as cmn
from collections import namedtuple
from ppi_client.ppi import PPI
from ppi_client.models.instrument import Instrument
import json

# Cocos wrapper
class CocosWrapper():
    def __init__(self, mm):
        self.SETTLEMENT_48HS = "48hs"
        self.SETTLEMENT_24HS = "24hs"
        self.SETTLEMENT_SPOT = "spot"
        self.default_settlement = self.SETTLEMENT_48HS

        self.broker_id = int(265) # COCOS
        self._marketmanager = mm
        self.hb = HomeBroker(broker_id = self.broker_id,
                   on_open = self.on_connection_open,
                   on_order_book = self.on_order_book,
                   on_close = self.on_connection_close,
                   on_error = self.on_error)
        self.connection_lost = False

    def is_logged_in(self):
        return self.hb.auth.is_user_logged_in

    def on_order_book(self, online, quotes):
        if not len(quotes):
            return

        mm_quotes = MM.OrderBookQuotes(quotes.index[0][0], quotes.index[0][1])

        ps = namedtuple("PriceSize", "price,size")
        mm_quotes.bids = list(map(lambda x, y: ps(x, y), quotes.bid, quotes.bid_size))
        mm_quotes.asks = list(map(lambda x, y: ps(x, y), quotes.ask, quotes.ask_size))

        self._marketmanager.on_order_book(mm_quotes, online)

    def on_connection_open(self, online):
        self._marketmanager.on_connection_open(online)

    def on_connection_close(self, online):
        self._marketmanager.on_connection_close(online)

    def on_error(self, online, exception, connection_lost):
        # <CONNECTION_LOST> Con self.connection_lost estoy cubriendo un error de pyhomebroker/signalr:
        # frente a connection_lost = True, is_connected() devuelve True
        self.connection_lost = connection_lost
        if not connection_lost and isinstance(exception, cmn.ServerException) and str(exception) == "NoLogin":
                logging.error("NoLogin error. Trying to re-login")
                self.logout()
                self.login()
        else:
             self._marketmanager.on_error(online, exception, connection_lost)

    def login(self):
        self.hb.auth.login( **creds.cocos_login_config, raise_exception=True )

    def logout(self):
        self.hb.auth.logout()

    def connect(self):
        self.hb.online.connect()

    def subscribe_order_book(self, ticker, settlement = None):
        if not settlement:
            settlement = self.default_settlement
        self.hb.online.subscribe_order_book(ticker, settlement)

    def disconnect(self):
        self.hb.online.disconnect()

    def unsubscribe_order_book(self, ticker, settlement=None):
        if not settlement:
            settlement = self.default_settlement
        self.hb.online.unsubscribe_order_book( ticker, settlement )

    def subscribe_personal_portfolio(self):
        self.hb.online.subscribe_personal_portfolio()

    def unsubscribe_personal_portfolio(self):
        self.hb.online.unsubscribe_personal_portfolio()

    def get_orders_status(self):
        """Devuelve una lista de named tuples con los campos ticker, number, status, price, datetime y done"""
        orders = self.hb.orders.get_orders_status( creds.cocos_account_id )
        # TODO: el namedtuple debería definirse en algún lado (config?)
        order_item = namedtuple("Order", "ticker,number,status,price,datetime,done")

        return [order_item(row.symbol, number, row.status, row.price, row.datetime, row["size"] - row.remaining_size)
                for number, row in orders.iterrows()]

    def get_last_close_price(self, ticker, date = datetime.date.today()):
        """Devuelve el valor del último cierre de un ticker"""

        # TODO: hay que garantizar que se obtiene el valor de cierre del último día operado.
        #  Un calendario de feriados que diga cuál fué el último día de actividad previo
        #  Por performance, tratar de obtener el valor inicialmente de la db, sino online

        # get_daily_history tiene un comportamiento raro: devuelve datos del día anterior al pasado salvo que sea lunes
        #  por eso decidí traer la última semana y quedarme con el último día. Capaz a futuro sirva mas tener mas datos
        info = self.hb.history.get_daily_history( ticker, date - datetime.timedelta(7), date)

        if len(info) == 0:
            raise ValueError(f"No hay info previa al {date} para {ticker}")

        return dict(info.iloc[-1])["close"]


    def send_sell_order(self, ticker, price, nominals, settlement):
        if not settlement:
            settlement = self.default_settlement
        return self.hb.orders.send_sell_order( ticker, settlement, float( price ), int( nominals ))

    def send_buy_order(self, ticker, price, nominals, settlement):
        if not settlement:
            settlement = self.default_settlement
        return self.hb.orders.send_buy_order( ticker, settlement, float(price), int(nominals))

    def cancel_order(self, order_number):
        self.hb.orders.cancel_order(creds.cocos_account_id, order_number)

    def get_current_assets(self):
        if not self.is_connected():
            raise cmn.SessionException('Not online')

        resp = None
        headers = {
            'User-Agent': cmn.user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        try:
            resp = requests.post("https://cocoscap.com/Consultas/GetConsulta",
                                 params={"comitente": creds.cocos_account_id, "proceso": "60", "consolida": "0"},
                                 cookies=self.hb.auth.cookies,
                                 headers=headers)

            resp.raise_for_status()

            if not resp["Success"]:
                raise Exception(resp["Error"]["Descripcion"])

        except requests.exceptions.HTTPError as http_err:
            logging.error(f'HTTP error: {http_err}')

        except Exception as err:
            logging.error(f'Other error: {err}')

        data = {}
        for asset in resp["Result"]["Detalle"]:
            asset = {k.lower(): v for k, v in asset.items()}
            data[asset["tick"].upper()] = asset["cant"] if asset["cant"] is not None else 0.00

        return data

    def get_last_price(self, symbol, settlement = None):
        if not settlement:
            settlement = self.default_settlement

        if not self.is_connected():
            raise cmn.SessionException('Not online')

        headers = {
            'User-Agent': cmn.user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/json; charset=UTF-8'
        }

        url = 'https://cocoscap.com/Prices/GetByStock'

        payload = {
            'symbol': symbol,
            'term': self.hb.online.get_settlement_for_request(settlement)
        }

        response = requests.post(url, json=payload, headers=headers, cookies=self.hb.auth.cookies)
        response.raise_for_status()

        response = response.json()

        if not response['Success']:
            raise Exception(response["Error"]["Descripcion"])

        if response['Result'] and response['Result']['Stock'] and response['Result']['Stock']['LastPrice'] and \
            response['Result']['Stock']['LastPrice'] != "-" :
            price = float(response['Result']['Stock']['LastPrice'].replace('.', '').replace(',','.'))
        else:
            price = None

        return price

    def is_connected(self):
        status = False
        if self.hb.online._signalr._connection is not None:
            # TODO: este debería ser el verdadero status de la conexión?
            #  Creo que pyhomebroker no actualiza bien su self._signalr.is_connected
            status = self.hb.online._signalr._connection.is_open
        else:
            status = self.hb.online.is_connected()
        return status

    # <CONNECTION_LOST> esto no debería existir, estoy cubriendo un error de pyhomebroker
    def reconnect(self):
        try:
            logging.info(f"Trying to reconnect due to connection_lost flag")
            self.hb.online._signalr.disconnect()
            self._marketmanager.connect()
            self.connection_lost = False
        except Exception as err:
            logging.error(f"During reconnection: {err}")


class HybridWrapper():
    def __init__(self, mm):
        self.SETTLEMENT_72HS = "A-72HS"
        self.SETTLEMENT_48HS = "A-48HS"
        self.SETTLEMENT_24HS = "A-24HS"
        self.SETTLEMENT_SPOT = "INMEDIATA"
        self.default_settlement = self.SETTLEMENT_48HS

        self._marketmanager = mm
        self.hb = PPI(sandbox=False)
        self.hb_session = None
        self.connection_assets = None

        self.broker_id = int(265) # COCOS
        self.hb2 = HomeBroker(broker_id = self.broker_id, on_error = self.on_hb_error)
        self.connection_lost = False
        self.cocos_default_settlement = "48hs"

    # region PPI - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def is_logged_in(self):
        #TODO: por ahora no encontré una forma de chequear el status de la conexión con la librería
        return self.hb_session != None

    def on_order_book(self, info):
        data = json.loads(info.lower())
        if data["trade"]:
            mm_quotes = MM.OrderBookQuotes(data["ticker"].upper(), data["settlement"], data["price"])
        else:
            mm_quotes = MM.OrderBookQuotes(data["ticker"].upper(), data["settlement"].upper())

            ps = namedtuple("PriceSize", "price,size")
            mm_quotes.bids = list(ps(x["price"], x["quantity"]) for x in data["bids"])
            mm_quotes.asks = list(ps(x["price"], x["quantity"]) for x in data["offers"])

        self._marketmanager.on_order_book(mm_quotes)

    def on_connection_open(self):
        if self.connection_assets:
            for ticker in self.connection_assets:
                self.hb.realtime.subscribe_to_element(Instrument(*self.get_ppi_ticker(ticker)))

            self.connection_assets = None

        self._marketmanager.on_connection_open(True)

    def on_connection_close(self):
        self._marketmanager.on_connection_close(False)

    def login(self):
        self.hb_session = self.hb.account.login_api(creds.ppi_login_config["public_PROD_key"],creds.ppi_login_config["private_PROD_key"])

    def logout(self):
        pass

    # Para compatibilizar con Cocos, que subscribe despues de conectar, ahora recibe en connect los assets para que se gestione segun el broker
    def connect(self, assets):
        self.connection_assets = assets
        self.hb.realtime.connect_to_market_data(self.on_connection_open, self.on_connection_close, self.on_order_book)
        logging.info("PPI connection set")

    def subscribe_order_book(self, ticker, settlement = None):
        self.hb.realtime.subscribe_to_element(*self.get_ppi_ticker(ticker, settlement))

    def disconnect(self):
        return

    def unsubscribe_order_book(self, ticker, settlement = None):
        pass

    def subscribe_personal_portfolio(self):
        pass

    def unsubscribe_personal_portfolio(self):
        pass

    def get_orders_status(self):
        """Devuelve una lista de named tuples con los campos ticker, number, status, price, datetime y done"""
        orders = self.hb2.orders.get_orders_status( creds.cocos_account_id )
        # TODO: el namedtuple debería definirse en algún lado
        order_item = namedtuple("Order", "ticker,number,status,price,datetime,done")

        return [order_item(row.symbol, number, row.status, row.price, row.datetime, row["size"] - row.remaining_size)
                for number, row in orders.iterrows()]

    def get_last_close_price(self, ticker, date = datetime.date.today(), settlement = None):
        market_data = self.hb.marketdata.search(*self.get_ppi_ticker(ticker,settlement), date - datetime.timedelta(7), date)
        if len(market_data) == 0:
            raise ValueError(f"No hay info previa")

        return market_data[-1]['price']


    def get_last_price(self, ticker, settlement=None):
        res = self.hb.marketdata.current(*self.get_ppi_ticker(ticker,settlement))

        return res["price"]

    def get_ppi_ticker(self, ticker, settlement= None):
        if settlement == None:
            settlement = self.default_settlement

        info = dblib.get_asset_info(ticker)

        return [info["ticker"], info["ppi_type"], settlement]

    # endregion

    #region COCOS - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def send_sell_order(self, ticker, price, nominals, settlement = None):
        if not settlement:
            settlement = self.cocos_default_settlement
        return self.hb2.orders.send_sell_order( ticker, settlement, float( price ), int( nominals ))

    def send_buy_order(self, ticker, price, nominals, settlement = None):
        if not settlement:
            settlement = self.cocos_default_settlement
        return self.hb2.orders.send_buy_order( ticker, settlement, float(price), int(nominals))

    def cancel_order(self, order_number):
        self.hb2.orders.cancel_order(creds.cocos_account_id, order_number)

    def get_current_assets(self):
        if not self.is_connected():
            raise cmn.SessionException('Not online')

        resp = None
        headers = {
            'User-Agent': cmn.user_agent,
            'Accept-Encoding': 'gzip, deflate',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        try:
            resp = requests.post("https://cocoscap.com/Consultas/GetConsulta",
                                 params={"comitente": creds.cocos_account_id, "proceso": "60", "consolida": "0"},
                                 cookies=self.hb2.auth.cookies,
                                 headers=headers)

            resp.raise_for_status()
            resp = resp.json()

            if not resp["Success"]:
                raise Exception(resp["Error"]["Descripcion"])

        except requests.exceptions.HTTPError as http_err:
            logging.error(f'HTTP error: {http_err}')

        except Exception as err:
            logging.error(f'Other error: {err}')

        data = {}
        for asset in resp["Result"]["Detalle"]:
            asset = {k.lower(): v for k, v in asset.items()}
            data[asset["tick"].upper()] = asset["cant"] if asset["cant"] is not None else 0.00

        return data

    # Heredado de COCOs
    def is_connected(self):
        status = False
        if self.hb2.online._signalr._connection is not None:
            # TODO: este debería ser el verdadero status de la conexión?
            #  Creo que pyhomebroker no actualiza bien su self._signalr.is_connected
            status = self.hb2.online._signalr._connection.is_open
        else:
            status = self.hb2.online.is_connected()
        return status

    def reconnect(self):
        try:
            logging.info(f"Trying to reconnect due to connection_lost flag")
            self.hb2.online._signalr.disconnect()
            self._marketmanager.connect()
            self.connection_lost = False
        except Exception as err:
            logging.error(f"During reconnection: {err}")

    def on_hb_error(self, online, exception, connection_lost):
        # <CONNECTION_LOST> Con self.connection_lost estoy cubriendo un error de pyhomebroker/signalr:
        # frente a connection_lost = True, is_connected() devuelve True
        self.connection_lost = connection_lost
        self._marketmanager.on_error(online, exception, connection_lost)

    #endregion