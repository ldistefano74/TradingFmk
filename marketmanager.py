import copy
import datetime
import Config as cfg
from strategies import *
import helpers
import dblib
import random
import logging
import brokerinterface as bi


class OrderBookQuotes:
    def __init__(self, ticker, settlement, last_price = None):
        self.ticker = ticker
        self.settlement = settlement
        self.bids = None    # Lista de named tuples PriceSize(price, size)
        self.asks = None    # Lista de named tuples PriceSize(price, size)
        self.last_price = last_price

class MarketManager:

    def __init__(self, timer_off = False):
        logging.info(f"(MarketManager) Initializing ...")
        self.load_time_config()
        self._previous_status = None

        self.hb = bi.HybridWrapper(self)

        logging.info(f"(MarketManager) Initializing ... broker access interface created")

        self.hb.login()

        logging.info(f"(MarketManager) Initializing ... logged in broker")

        for k, v in Config.asset_strategies.items():
            # logging.info(f"(MarketManager) Instantiating ... {v['ticker']}")
            v["instance"] = globals()[v["class"]]( self, v["ticker"], v, k )

        logging.info(f"(MarketManager) Initializing ... strategies created")

        # al iniciar, actualizar el estado de las órdenes preexistentes
        self.load_orders()
        # TODO: restore check_orders_status
        # self.check_orders_status()

        logging.info(f"(MarketManager) Initializing ... orders loaded and checked")

        # TODO : levantar el last_bid / ask de cada ticker (lo haría al conectar a través de on_order_book??)

        self.timer = helpers.BrokerTimer(cfg.timer_tick, self.on_timer_tick)
        logging.info(f"(MarketManager) Initializing ... timer created ({'OFF' if timer_off else 'ON'})")
        if not timer_off:
            self.timer.start()


    def load_orders(self):
        for order in dblib.get_orders():
            Config.asset_strategies[order.strategy_id]["instance"].daily_orders.append(order)


    def load_time_config(self):
        if cfg.pre_market_start and cfg.market_start < cfg.pre_market_start:
            raise ValueError( f"Config error: Pre market ({cfg.pre_market_start}) " 
                             f"no puede ser superior a market start ({cfg.market_start})")

        if cfg.post_market_end and cfg.market_end > cfg.post_market_end:
            raise ValueError( f"Config error: Post market ({cfg.post_market_end}) "
                             f"no puede ser inferior a market end ({cfg.market_end})" )

        self.market_start = cfg.market_start
        self.pre_market_start = cfg.pre_market_start
        self.market_end = cfg.market_end
        self.pre_market_end = cfg.pre_market_end
        self.post_market_end = cfg.post_market_end

    def on_timer_tick(self):
        prev = self._previous_status
        new = self.market_status
        logging.info(f"Market Status: {new}, Connected: {self.hb.is_connected()} - Logged in: {self.hb.is_logged_in()}")

        #  TODO: chequear el estado de conexión para eventualmente evitar disparar eventos?
        # <CONNECTION_LOST>
        if self.hb.connection_lost:
            self.hb.reconnect()

        # TODO: debería chequearse el estado de órdenes en cada tick?
        # self.check_orders_status()

        self.run_strategies_tick(new, prev)

        return

    @property
    def market_status(self):
        # CLOSED, PRE, OPEN, PRE_CLOSE, POST, CLOSED
        now = helpers.get_time()

        if now < self.pre_market_start:
            status = "CLOSED"

        elif self.pre_market_start <= now < self.market_start:
            status = "PRE"

        elif self.market_start <= now < self.pre_market_end:
            status = "OPEN"

        elif self.pre_market_end <= now < self.market_end:
            status = "PRE_CLOSE"

        elif self.market_end <= now < self.post_market_end:
            status = "POST"

        else:
            status = "CLOSED"

        self._previous_status = status

        return status

    def run_strategies_tick(self, status, previous):
        for strat in Config.asset_strategies.values():
            strat["instance"].on_timer_tick(status, previous)
        return

    def connect(self):
        self.hb.connect()

        assets = set( [x["ticker"] for x in Config.asset_strategies.values()] )
        for asset in assets:
            self.hb.subscribe_order_book( asset )

    def disconnect(self):
        assets = set( [x["ticker"] for x in Config.asset_strategies.values()] )
        for asset in assets:
            self.hb.unsubscribe_order_book( asset )

        self.hb.disconnect()

    def on_order_book(self, quotes:OrderBookQuotes, online = True):
        if self.market_status in ("OPEN", "PRE_CLOSE"):
            for strat in Config.asset_strategies.values() :
                if strat["ticker"] == quotes.ticker:
                    strat["instance"].on_order_book_change(quotes)

    def on_connection_open(self, online):
        logging.info( '=================== CONNECTION OPENED ====================' )

    def on_connection_close(self, online):
        logging.info( '=================== CONNECTION CLOSED ====================' )

    def check_orders_status(self, trigger = True):
        for hborder in self.hb.get_orders_status():
            order, strat = self.find_order(hborder.ticker, hborder.number)
            if order:
                original = copy.copy(order)
                order.order_status = hborder.status
                order.price = hborder.price
                order.datetime = hborder.datetime
                order.done_nominals = hborder.done

                if order != original:
                    dblib.save_order(order)
                    if trigger:
                        strat.on_order_change(order)

    def find_order(self, ticker, order_number):
        for strat in Config.asset_strategies.values():
            if strat["ticker"] == ticker and strat["instance"]:
                for order in strat["instance"].daily_orders:
                    if order.order_number == order_number:
                        return order, strat["instance"]
        return None, None

    def on_error(self, online, exception, connection_lost):
        logging.error(f"* Manager * {type(exception)} {exception} - Connection_lost: {connection_lost} - Connected: {self.hb.is_connected()}")
        if "NoLogin" in str(exception):
            logging.info(f"NoLogin detected, attempting to re-login")
            self.hb.logout()
            self.hb.login()

    def get_prev_info(self, ticker, date = datetime.date.today()):
        return self.hb.get_last_close_price(ticker, date)

    def put_order(self, order:dblib.Order):
        if order.nominals <= 0:
            raise  ValueError(f"Put order needs nominals > 0")

        if order.price <= 0:
            raise  ValueError(f"Put order needs price > 0")

        if not order.order_status:
            order.order_status = "SENT"

        if Config.mode == "SIM":
            order.order_number = str( int( random.random() * 10_000_000 ) )
        # else:
        #     try:
        #         if order.order_type in ("SELL", "SL"):
        #             order.order_number = self.hb.send_sell_order( order.ticker, order.price, order.nominals)
        #         else:
        #             order.order_number = self.hb.send_buy_order( order.ticker, order.price, order.nominals)
        #         if order.order_number:
        #             order.order_status = "OFFERED"
        #     except Exception as exc :
        #         logging.error(f"MarketManager put_order error:{exc}")
        #         return False

        dblib.save_order(order)

        return True


    def cancel_order(self, order:dblib.Order):

        if Config.mode == "REAL" and order.order_number:
            self.hb.cancel_order(order.order_number)

        order.order_status = "CANCELLED"
        dblib.save_order( order )


    def get_current_assets(self):

        return self.hb.get_current_assets()

    def status_report(self):
        report = f"{helpers.get_time()}\nMarket status: {self.market_status}\n"
        report += f"Timer status: {self.timer.status()}\n"
        report += f"Broker status: \n"
        report += f"\tConnected: {self.hb.is_connected()}\n"
        report += f"\tLogged in: {self.hb.is_logged_in()}\n"
        report += f"Strategies: \n"
        for id, strat in Config.asset_strategies.items():
            report += f"\t{id} ({strat['instance'].ticker}): " + strat['instance'].status_report() + "\n"

        return report

if __name__ == "__main__":
    x = MarketManager(timer_off=True)