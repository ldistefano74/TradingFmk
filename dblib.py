import datetime
import creds

import psycopg2
from psycopg2.extras import RealDictCursor

import Config
import helpers

from functools import lru_cache

def get_dbconnection():
    return psycopg2.connect(**creds.db_conn_info, cursor_factory=RealDictCursor)

class dbconnection:
    def __init__(self):
        self.connection = None

    def __enter__(self):
        self.connection =  psycopg2.connect( **creds.db_conn_info, cursor_factory=RealDictCursor)
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()
        if exc_type:
            print(f'dblib Error: {exc_val}')


class Order:
    def __init__(self, ticker, order_type, nominals, price, order_status = None, order_number = None):
        self.ticker = ticker
        self.order_type = order_type        # BUY, SELL, SL
        self._order_status = None    # TODO: definir constantes None, 'SENT', 'PENDING', 'OFFERED', 'PARTIAL', 'COMPLETED', 'CANCELLED', 'REJECTED'
        self._last_order_status = None
        self.order_status = order_status
        self.nominals = nominals
        self.done_nominals = 0
        self.order_number = order_number
        self.id = None
        self.price = price
        self.strategy_id = ""
        self.datetime = None


    def __eq__(self, other):
        ret = type(self) == type(other)
        if ret:
            try:
                ret = self.__dict__ == other.__dict__
            except:
                pass
        return ret

    @property
    def active(self):
        return self.order_status in ('SENT', 'PENDING', 'OFFERED', 'PARTIAL')

    @property
    def order_status(self):
        return self._order_status

    @order_status.setter
    def order_status(self, value):
        self._order_status = value
        self._last_order_status = helpers.get_time().replace(microsecond=0)

    def status_report(self):
        return f"{self.order_type} id: {self.id} / status: {self.order_status} ({self._last_order_status}) / price: {self.price} / nominals: {self.nominals} / done: {self.done_nominals}"

def save_order(order:Order):
    order.nominals = int( order.nominals )
    order.done_nominals = int( order.done_nominals )
    order.price = float( order.price )

    if Config.mode == "SIM" and order.active:
        if order.done_nominals == order.nominals:
            order.order_status = "COMPLETED"
        elif 0 < order.done_nominals <= order.nominals:
            order.order_status = "PARTIAL"

    with dbconnection() as conn:
        cursor = conn.cursor()

        query = "INSERT INTO orders(datetime, asset_id, order_type, order_status, nominals, done_nominals, price, order_number, strategy_id)\
                (SELECT %(datetime)s, id, %(order_type)s, %(order_status)s, %(nominals)s, %(done_nominals)s, %(price)s, %(order_number)s,\
                 %(strategy_id)s\
                 FROM assets WHERE ticker = %(ticker)s) RETURNING id" \
                if not order.id else \
                "UPDATE orders SET datetime = %(datetime)s, order_status = %(order_status)s, nominals = %(nominals)s,  \
                done_nominals = %(done_nominals)s, price = %(price)s, order_number = %(order_number)s WHERE id = %(id)s RETURNING id"

        data = {"order_type": order.order_type, "order_status": order.order_status, "nominals": order.nominals,
                "done_nominals": order.done_nominals, "price": float(order.price), "order_number": order.order_number, "ticker": order.ticker,
                "id": order.id, "strategy_id": order.strategy_id, "datetime" : (order.datetime or datetime.datetime.now())}

        cursor.execute(query, data)
        conn.commit()

        order.id = cursor.fetchone()["id"]

    return


def get_orders(date = datetime.date.today()):
    with dbconnection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT O.*, A.Ticker FROM Orders O JOIN Assets A ON O.Asset_id = A.id WHERE DATE(datetime) = %s", [date])

        orders = []
        for row in cursor.fetchall():
            order = Order("", "", 0, 0, order_status=row["order_status"]) #El status es una @property, no forma parte de __dict__
            order.__dict__.update((k, v) for k, v in row.items() if k in order.__dict__)
            orders.append(order)

    return orders

@lru_cache(maxsize=None)
def get_asset_info(ticker):
    with dbconnection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT A.ticker, A.ppi_id, A.ppi_type FROM Assets A WHERE ticker = %s limit 1", [ticker])
        row = cursor.fetchone()

    return row if row and len(row) else None


if __name__ == "__main__":
    print(get_asset_info("ARCO"))