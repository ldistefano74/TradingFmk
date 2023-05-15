import datetime

import Config
import dblib
from dblib import Order
import logging
import helpers
from collections import namedtuple
import brokerinterface as bi

class Strategy:
    def __init__(self, market, ticker, strategy_id):
        self.market = market
        self.ticker = ticker
        self.strategy_id = strategy_id
        self.last_close_price = market.get_prev_info(ticker)
        self.daily_orders = []
        self.LBA = namedtuple("LastBidAsk", "price,size,time")
        self.last_bid = self.LBA(0.0, 0, None)
        self.last_ask = self.LBA(0.0, 0, None)
        self.last_price = None

    def on_timer_tick(self, status, previous_status):

        if status == "PRE":
            self.on_pre_market()

        elif status == "OPEN":
            if previous_status == "PRE":
                self.on_market_open()

        elif status == "PRE_CLOSE":
            if previous_status == "OPEN":
                self.on_pre_market_close()

        elif status == "POST":
            if previous_status == "PRE_CLOSE":
                self.on_market_close()
            else:
                self.on_post_market()

        return

    def on_pre_market(self):
        return

    def on_market_open(self):
        # IMPORTANTE: en caso de reinicio, este metodo no se va a llamar, solo sucede en el paso de PRE a OPEN
        return

    def on_order_book_change(self, quotes):
        time = helpers.get_time().replace(microsecond=0)
        if quotes.bids and self.last_bid.price != quotes.bids[0].price:
            self.last_bid = self.LBA(quotes.bids[0].price, quotes.bids[0].size, time)
        if quotes.asks and self.last_ask.price != quotes.asks[0].price:
            self.last_ask = self.LBA(quotes.asks[0].price, quotes.asks[0].size, time)
        if quotes.last_price:
            self.last_price = quotes.last_price
        return

    def on_order_executed(self, order_id, value):
        return

    def on_market_close(self):
        return

    def on_pre_market_close(self):
        return

    def on_post_market(self):
        return

    def on_order_change(self, order):
        if Config.mode == "SIM":
            self.calculate_current_nominals()
        return

    def put_order(self, order):
        order.strategy_id = self.strategy_id
        ret = self.market.put_order(order)
        if ret:
            self.daily_orders.append(order)
            logging.info(f"({self.strategy_id}) {self.ticker} Put {order.order_type} order internal id: {order.id}")

        return ret

    def calculate_current_nominals(self):
        # TODO: comparar los nominales por orden con los current a modo de validacion + log
        nominals = sum(order.done_nominals if order.order_type == "BUY" else -order.done_nominals
                       for order in self.daily_orders)

        return nominals

    def get_current_nominals(self):
        nominals = 0
        if Config.mode == "SIM":
            nominals = self.calculate_current_nominals()
        else:
            assets = self.market.get_current_assets()
            nominals = 0 if not self.ticker in assets.keys() else assets[self.ticker]

        return nominals

    def cancel_orders(self, *args):
        self.market.check_orders_status(trigger = False)
        for order in self.daily_orders:
            if order.order_type in args and\
                    order.order_number and \
                    order.active:

                logging.info(f"({self.__class__.__name__}) {self.ticker} Cancelling order id {order.id}")
                self.market.cancel_order(order)

    def sell(self, price, nominals = None, type = "SELL", cancel_prev = True, done = 0):
        if price <= 0:
            raise ValueError( f"SELL: el precio debe ser superior a 0")

        cur_nominals = self.get_current_nominals()
        if nominals:
            nominals = min(nominals, cur_nominals)
        else:
            nominals = cur_nominals

        if nominals:
            if cancel_prev:
                # Si alcancé un target de venta (por SL o target) cancelo incluso una eventual compra parcial
                self.cancel_orders("SELL", "SL", "BUY")

            new_order = Order(self.ticker, type, nominals, price)
            if Config.mode == "SIM":
                new_order.done_nominals = min(nominals, done)

            logging.info(f"({self.strategy_id}) {self.ticker} {type} at {new_order.price} with {new_order.nominals} nominals ({new_order.done_nominals} done)" )
            if not self.put_order(new_order):
                logging.warning(f"({self.strategy_id}) {self.ticker} ERROR putting {type}")
            return new_order

        return None

    def buy(self, nominals, price, done = 0):
        new_order = Order(self.ticker, "BUY", nominals, price)
        logging.info(
            f"({self.strategy_id}) {self.ticker} BUY at {new_order.price} with {new_order.nominals} nominals ({new_order.done_nominals} done)")
        if not self.put_order(new_order):
            logging.warning(f"({self.strategy_id}) {self.ticker} ERROR putting BUY")
        return new_order

    def status_report(self, extra_info = ""):
        report = f"\tcurrent nominals:{self.get_current_nominals()} / " \
                 f"last bid {self.last_bid.time} : {self.last_bid.price}({self.last_bid.size}) / " \
                 f"last ask {self.last_ask.time} : {self.last_ask.price}({self.last_ask.size}) / " \
                 f"last close price: {self.last_close_price}" + extra_info
        if len(self.daily_orders):
            for order in sorted(self.daily_orders, key = lambda x:x.id):
                report += f"\t\t{order.status_report()}\n"
        else:
            report += "\t\tNo orders"
        return report


class Strategy1(Strategy):

    def __init__(self, market, ticker, strategy_config, strategy_id):
        super(Strategy1, self).__init__(market, ticker, strategy_id)
        self.last_order_book = self.buy_target = self.sl_threshold = self.sell_target = None

        self.buy_threshold = strategy_config["buy_threshold"]
        self.sl_threshold = strategy_config["sl_threshold"]
        self.win_target = strategy_config["win_target"]
        self.nominals_target = strategy_config["nominals_target"]
        self.stop_buy_time = strategy_config["stop_buy_time"]

        #  TODO: en miles tal vez convenga redondear a multiplos de 5
        self.buy_target = helpers.round_price(float(self.last_close_price) * self.buy_threshold)
        self.sell_target = helpers.round_price( self.buy_target * self.win_target )
        self.sl_target = helpers.round_price( self.buy_target * self.sl_threshold )

        return

    def on_order_book_change(self, quotes):
        super(Strategy1, self).on_order_book_change(quotes)

        if quotes.last_price:
            return

        current_nominals = self.get_current_nominals()

        # TODO: esto no debería estar llamandose directo al broker. Lo ideal sería mandar un default, ya que con PPI
        #  se setea desde la clase padre
        if type(self.market.hb) is bi.CocosWrapper or self.last_price is None:
            self.last_price = self.market.hb.get_last_price(self.ticker)

        # si está simulando, al llegar al precio de compra u oferta asume que las ordenes de compra se ejecutan
        # por la cantidad de nominales ofertada
        if Config.mode == "SIM":
            best_price = min(self.last_ask.price, self.last_price) if self.last_price else self.last_ask.price
            if self.sl_target < best_price <= self.buy_target:
                for order in self.daily_orders:
                    if order.order_type == "BUY" and order.active:
                        order.done_nominals += min(self.last_ask.size, order.nominals - order.done_nominals)
                        logging.info(
                            f"({self.strategy_id}) {self.ticker} -SIM MODE- BUY target reached, "
                            f"cur nominals:{current_nominals} / ask = {self.last_ask.price} / "
                            f"target = {self.buy_target} / {order.done_nominals} done")
                        dblib.save_order(order)

                current_nominals = self.get_current_nominals()

        best_price = max(self.last_bid.price, self.last_price) if self.last_price else self.last_bid.price
        # si la oferta actual alcanzó el SL o el sell target, cancelar cualquier orden pendiente y vender todos los nominales que tenga
        if current_nominals and (self.last_bid.price >= self.sell_target or best_price <= self.sl_target):
            if self.last_bid.price >= self.sell_target:
                order_type = "SELL"
                target = self.sell_target
            else:
                order_type = "SL"
                target = self.sl_target

            order = self.sell(best_price, type = order_type, done = self.last_bid.size)
            logging.info(f"({self.strategy_id}) {self.ticker} {order_type} target reached, "
                         f"cur nominals:{current_nominals} / bid = {self.last_bid.price} / target = {target} /"
                         f" {order.done_nominals} done")


    def on_pre_market(self):
        if not any(x for x in self.daily_orders if x.order_type == "BUY" and x.active ):
            self.buy(self.nominals_target - self.get_current_nominals(), self.buy_target)

    def on_timer_tick(self, status, previous_status):
        super( Strategy1, self ).on_timer_tick(status, previous_status)

        # Para las 16, si no se compraron, hay que cancelar las ordenes de compra pendientes
        if helpers.get_time() >= self.stop_buy_time and \
                any(True for x in self.daily_orders if x.order_type == "BUY" and x.active ):
            logging.info(f"{self.strategy_id} * Cancelling pending BUY orders")
            self.cancel_orders("BUY")

        if status == "PRE_CLOSE":
            self.sell_pre_market_close()

    def sell_pre_market_close(self):
        # cerrando el market vender las posiciones abiertas
        if self.get_current_nominals():
            # EXCEPCION: en SIM MODE, tratando de cerrar los stocks por falta de conexion con el broker al terminar el día
            # self.sell(self.market.hb.get_last_price(self.ticker), done=self.get_current_nominals())
            if self.last_bid.price:
                self.sell(self.last_bid.price, done=self.last_bid.size)
            else:
                logging.warning(f"({self.strategy_id}) {self.ticker} ERROR: no hay ultimo precio de oferta para vender en PRE_CLOSE")

    def status_report(self, extra_info=""):
        extra = f" / Buy target: {self.buy_target} / Sell target: {self.sell_target} / SL target: {self.sl_target} \n"
        return super(Strategy1, self).status_report(extra)

