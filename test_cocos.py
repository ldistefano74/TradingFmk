# Este testeo debería quedar deprecado y utilizarse únicamente test_ppi.
# Eventualmente se debería compatibilizar la CocosWrapper
import logging
import unittest.mock as mock
import pandas
import copy

import Config
import creds
import datetime
import psycopg2

import helpers
from marketmanager import MarketManager
import brokerinterface as bi
import dblib


def test_connection():
    mm = MarketManager(timer_off=True)
    logging.info(f"is_connected : {mm.hb.hb.online.is_connected()}")
    mm.connect()
    logging.info(f"is_connected : {mm.hb.hb.online.is_connected()}")
    mm.disconnect()
    logging.info(f"is_connected : {mm.hb.hb.online.is_connected()}")

def test_conn_cfg():
    print("Testing db connection ...")
    print (creds.db_conn_info)
    try :
        with dblib.dbconnection() as conn:
            assert conn.status == psycopg2.extensions.STATUS_READY, f"Connection status is not ready: {conn.status}"

    except Exception as err:
        print(f"Error: {err}")

def mock_books_quotes (ticker, bid, ask, bid_size = 1, ask_size = 1, bid_count = 1, ask_count = 1):
    order_book_index = ['symbol', 'settlement', 'position']
    order_book_buy_columns = ['position', 'bid_size', 'bid', 'bid_offers_count']
    order_book_sell_columns = ['position', 'ask_size', 'ask', 'ask_offers_count']
    order_book_columns = list(set(order_book_index) | set(order_book_buy_columns) | set(order_book_sell_columns))

    data = {"symbol": ticker, "settlement": bi.default_settlement, "position" : 1, "bid":bid, "ask": ask, "bid_offers_count": bid_count, "ask_offers_count": ask_count, "ask_size":ask_size, "bid_size": bid_size}

    return pandas.DataFrame(data, columns = order_book_columns, index=[0] ).set_index( order_book_index )

def mock_orders_quotes(orders):
    data = {"order_number": [], "symbol": [], "settlement": [], "operation_type": [], "size": [],
            "price": [], "remaining_size": [], "datetime": [], "status": [], "cancellable": []}
    for order in orders:
        data["order_number"].append(order.order_number)
        data["symbol"].append(order.ticker)
        data["settlement"].append( bi.default_settlement )
        data["operation_type"].append( order.order_type if order.order_type != "SL" else "SELL" )
        data["size"].append( order.nominals )
        data["price"].append( order.price )
        data["remaining_size"].append( order.nominals - order.done_nominals )
        data["datetime"].append( datetime.datetime.now() )
        data["status"].append( order.order_status )
        data["cancellable"].append( order.order_status in (None, 'SENT', 'PENDING', 'OFFERED', 'PARTIAL') )

    return pandas.DataFrame(data).set_index("order_number")

def test_order_comparison():
    o1 = dblib.Order( "ARCO", "BUY", 10, 100.0, "SENT", 1 )
    assert o1 != "text", "Type comparison failed"

    o2 = dblib.Order( "ARCO", "BUY", 10, 100.0, "SENT", 1 )
    assert o1 == o2, "Exact order comparison failed"

    o2.order_status = "CANCELLED"
    assert o1 != o2, "Non equal order comparison failed"


def test_order_status():
    o1 = dblib.Order( "ARCO", "BUY", 10, 100.0, "SENT", 1 )

    with mock.patch("helpers.get_time") as gt:
        gt.return_value = datetime.time(10, 54, 10)

        o1.order_status = "COMPLETED"
        report = o1.status_report()
        report_ref = "BUY id: None / status: COMPLETED (10:54:10) / price: 100.0 / nominals: 10 / done: 0"

    assert report == report_ref, f"El reporte debería decir {report_ref} y dice {report}"

def test_time_config():
    with mock.patch("brokerinterface.HomeBroker") as hb:
        with mock.patch("marketmanager.MarketManager.get_prev_info") as gpi:
            with mock.patch("helpers.get_time") as gt:
                gpi.return_value = 2000.0

                mm = MarketManager(timer_off=True)

                gt.return_value = datetime.time(10, 54, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(10, 55, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(16, 55, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(17, 4, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(17, 30, 00)
                mm.on_timer_tick()


def test_pre_market(autage = False):
    helper_clean_orders()

    with mock.patch("brokerinterface.HomeBroker") as hb:
        with mock.patch("marketmanager.MarketManager.get_prev_info") as gpi:
            with mock.patch("helpers.get_time") as gt:
                gpi.return_value = 2000.0

                mm = MarketManager(timer_off=True)

                gt.return_value = datetime.time(10, 54, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(10, 55, 00)
                mm.on_timer_tick()

                if autage:
                    del mm
                    for i in Config.asset_strategies.values():
                        i["instance"] = None
                    mm = MarketManager(timer_off=True)


                gt.return_value = datetime.time(10, 59, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(11, 00, 00)
                mm.on_timer_tick()

                orders = helper_get_orders(
                    "DATE(datetime) = %s AND order_status = 'SENT' AND order_type = 'BUY'", datetime.date.today())

                assert len(orders) == len(Config.asset_strategies), \
                    f"Las órdenes generadas ({len(orders)}) no son las que deberían ({len(Config.asset_strategies)})"

def test_sell():
    helper_clean_orders()

    with mock.patch("brokerinterface.HomeBroker") as hb:
        with mock.patch("marketmanager.MarketManager.get_prev_info") as gpi:
            with mock.patch("helpers.get_time") as gt:
                gpi.return_value = 2000.0

                mm = MarketManager(timer_off=True)

                gt.return_value = datetime.time(10, 55, 00)
                mm.on_timer_tick()

                order = x = list(Config.asset_strategies.values())[0]["instance"].sell(1000, 10)
                assert order is None, "Se generó al menos una orden de venta cuando no hay nominales por vender"

                gt.return_value = datetime.time(11, 10, 00)
                mm.on_timer_tick()


def test_buy():
    pass

def helper_clean_orders():
    with dblib.dbconnection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM Orders WHERE DATE(datetime) = %s",
            [datetime.date.today()])
        conn.commit()

def helper_get_orders(where_cond = "true", *args):
    with dblib.dbconnection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM Orders O WHERE " + where_cond + " ORDER BY id",
            [*args])

    return cursor.fetchall()

def test_on_order_book_change():
    helper_clean_orders()
    prev_close_value = 2000.0
    buy_threshold = .985

    with mock.patch("brokerinterface.HomeBroker") as hbm:
        # setear estrategias
        with mock.patch("Config.asset_strategies", {
                "ARCO_1": {"ticker": "ARCO", "class": "Strategy1", "buy_threshold": buy_threshold, "sl_threshold": .97,
                           "win_target": 1.02, "nominals_target": 10, "stop_buy_time":datetime.time(hour=16, minute = 0)},
                "BBAR_1": {"ticker": "BBAR", "class": "Strategy1", "buy_threshold": buy_threshold, "sl_threshold": .97,
                           "win_target": 1.02, "nominals_target": 10, "stop_buy_time":datetime.time(hour=16, minute = 0)}}):
            hb = hbm.return_value

            # abrir el mercado para que se registren las órdenes
            with mock.patch( "marketmanager.MarketManager.get_prev_info" ) as gpi:
                gpi.return_value = prev_close_value
                mm = MarketManager(timer_off=True)

            with mock.patch("helpers.get_time") as gt:
                gt.return_value = datetime.time(10, 55, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(11, 1, 00)
                mm.on_timer_tick()

                quotes = mock_books_quotes("ARCO", 1950, 2000, 3, 5)
                mm.hb.on_order_book(True, quotes)

            orders = helper_get_orders(
                "DATE(datetime) = current_date AND order_type in ('BUY') AND strategy_id = 'ARCO_1' AND done_nominals=3")
            assert len(orders) != 0, f"No se actualizó la orden, debería tener 3 done nominals"

def test_status_report():
    helper_clean_orders()
    prev_close_value = 2000.0
    buy_threshold = .985

    with mock.patch("brokerinterface.HomeBroker") as hbm:
        # setear estrategias
        with mock.patch("Config.asset_strategies", {
                "ARCO_1": {"ticker": "ARCO", "class": "Strategy1", "buy_threshold": buy_threshold, "sl_threshold": .97,
                           "win_target": 1.02, "nominals_target": 10},
                "BBAR_1": {"ticker": "BBAR", "class": "Strategy1", "buy_threshold": buy_threshold, "sl_threshold": .97,
                           "win_target": 1.02, "nominals_target": 10}}):
            hb = hbm.return_value

            # abrir el mercado para que se registren las órdenes
            with mock.patch( "marketmanager.MarketManager.get_prev_info" ) as gpi:
                gpi.return_value = prev_close_value
                mm = MarketManager(timer_off=True)

            with mock.patch("helpers.get_time") as gt:
                gt.return_value = datetime.time(10, 55, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(11, 1, 00)
                mm.on_timer_tick()

            import time
            print(mm.status_report())

def test_reaching_target(target_type, done_percent = 100, confirm_buy = "ORDER", cancel_on_EOM = False, outage = False):

    assert cancel_on_EOM and done_percent < 100.0 or not cancel_on_EOM, \
        "Para probar la cancelación en cierre de Market es necesario done_percent < 100"

    helper_clean_orders()
    prev_close_value = 2000.0
    buy_threshold = .985

    with mock.patch("brokerinterface.HomeBroker") as hbm:
        # setear estrategias
        with mock.patch("Config.asset_strategies", {
                "ARCO_1": {"ticker": "ARCO", "class": "Strategy1", "buy_threshold": buy_threshold, "sl_threshold": .97,
                           "win_target": 1.02, "nominals_target": 10},
                "BBAR_1": {"ticker": "BBAR", "class": "Strategy1", "buy_threshold": buy_threshold, "sl_threshold": .97,
                           "win_target": 1.02, "nominals_target": 10}}):
            hb = hbm.return_value

            # abrir el mercado para que se registren las órdenes
            with mock.patch( "marketmanager.MarketManager.get_prev_info" ) as gpi:
                gpi.return_value = prev_close_value
                mm = MarketManager(timer_off=True)

            with mock.patch("helpers.get_time") as gt:
                gt.return_value = datetime.time(10, 54, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(10, 55, 00)
                mm.on_timer_tick()

                gt.return_value = datetime.time(11, 5, 00)
                mm.on_timer_tick()

                # testear una caída: el market manager se crea de nuevo y debería seguir como si nada
                if outage:
                    del(mm)
                    for i in Config.asset_strategies.values():
                        i["instance"] = None
                    with mock.patch( "marketmanager.MarketManager.get_prev_info" ) as gpi:
                        gpi.return_value = prev_close_value
                        mm = MarketManager(timer_off=True)
                    gt.return_value = datetime.time(11, 5, 00)
                    mm.on_timer_tick()

                sold_nominals = max( int( Config.asset_strategies["ARCO_1"]["nominals_target"] * done_percent / 100 ), 1 )
                if confirm_buy == "ORDER":
                    # crear novedades de órdenes de compra cumplidas
                    test_orders = []
                    for strat in Config.asset_strategies.values():
                        for order in strat["instance"].daily_orders:
                            if order.order_type == "BUY" and order.order_status == "SENT":
                                order_copy = copy.copy(order)
                                order_copy.datetime = datetime.datetime.now()
                                order_copy.done_nominals = sold_nominals
                                order_copy.order_status = "COMPLETED" if done_percent == 100 else "PARTIAL"
                                test_orders.append(order_copy)

                    hb.orders.get_orders_status.return_value = mock_orders_quotes(test_orders)
                    mm.check_orders_status()

                elif confirm_buy == "BOOK":
                    quotes = mock_books_quotes( "ARCO", 1950, 1970, 20, sold_nominals )
                    mm.hb.on_order_book( True, quotes )

                offer = 20 if not cancel_on_EOM else 1

                if target_type == "SELL":
                    # el cambio de precio debería disparar la orden de venta con los nominales que tenga
                    quotes = mock_books_quotes("ARCO", 2050, 2060, offer, 20)
                    mm.hb.on_order_book(True, quotes)

                elif target_type == "SL":
                    quotes = mock_books_quotes("ARCO", 1900, 1910, offer, 20)
                    mm.hb.on_order_book(True, quotes)

                orders = helper_get_orders("DATE(datetime) = %s AND order_type in ('SELL', 'SL') AND strategy_id = 'ARCO_1'", datetime.date.today())
                if len(orders):
                    assert orders[0]["nominals"] == sold_nominals, f"La orden de venta se creó por {orders[0]['nominals']} cuando deberían ser {sold_nominals}"
                else:
                    raise AssertionError("No se generaron ordenes de venta ni SL")

                if done_percent != 100:
                    orders = helper_get_orders( "DATE(datetime) = %s AND order_status = 'CANCELLED' AND strategy_id = 'ARCO_1'", datetime.date.today())
                    assert len( orders ) == 1, f"No hubo ordenes canceladas con una compra parcial {done_percent}. Debería haber al menos una"

                orders = helper_get_orders( "DATE(datetime) = %s AND strategy_id = 'ARCO_1'", datetime.date.today() )
                print("Ordenes de ARCO_1 previas al cierre:")
                for order in orders:
                    print(f"   order_id: {order['id']} {order['strategy_id']} {order['order_type']} {order['order_status']} nominals:{order['nominals']} done:{order['done_nominals']} price:{order['price']}")

                # liquidar los nominales antes del cierre de mercado
                if cancel_on_EOM:
                    # simular el pre cierre con mas de un tick y venta parcial
                    gt.return_value = datetime.time(16, 55, 00)
                    mm.on_timer_tick()

                    # primero 3 a un precio
                    bid, bid_s, ask, ask_s = 2008, 3, 2020, 5
                    quotes = mock_books_quotes( "ARCO", bid, ask, bid_s, ask_s )
                    mm.hb.on_order_book( True, quotes )

                    # Chequear que los últimos bid y ask se está registrando
                    strat = Config.asset_strategies["ARCO_1"]["instance"]
                    assert strat.last_bid.price == bid, "No se actualizó el last bid"
                    assert strat.last_ask.price == ask, "No se actualizó el last ask"
                    assert strat.last_bid.size == bid_s, "No se actualizó el last bid size"
                    assert strat.last_ask.size == ask_s, "No se actualizó el last ask size"

                    orders = helper_get_orders("DATE(datetime) = %s AND order_status = 'PARTIAL' AND strategy_id = 'ARCO_1'", datetime.date.today())
                    assert len(
                        orders ) == 1, f"EOM debería haber una orden ejecutada parcialmente, hay {len( orders )}"

                    # despues otras 2 a otro precio
                    quotes = mock_books_quotes( "ARCO", 2000, 2015, 2, 5 )
                    mm.hb.on_order_book( True, quotes )

                    nominals = Config.asset_strategies["ARCO_1"]["instance"].get_current_nominals()
                    assert nominals == 0, f"No se vendieron todos los nominales, quedaron {nominals}"

                    orders = helper_get_orders( "DATE(datetime) = %s AND strategy_id = 'ARCO_1'", datetime.date.today() )
                    print("Ordenes de ARCO_1 al cierre para EOM:")
                    for order in orders:
                        print(f"   {order['id']} {order['strategy_id']} {order['order_type']} {order['order_status']} nominals:{order['nominals']} done:{order['done_nominals']} price:{order['price']}")



@mock.patch("creds.db_conn_info", {"host" : "localhost", "database" : "broker_test", "user" : "broker", "password" : "broker2021"})
@mock.patch("Config.mode", "SIM")
def test():
    pandas.set_option('display.max_rows', None)
    pandas.set_option('display.max_columns', None)
    pandas.set_option('display.width', None)

    # test_conn_cfg()
    # test_time_config()

    # test_order_comparison()
    test_order_status()

    # test_on_order_book_change()

    # test_connection()

    # test_status_report()

    # test_pre_market()
    # test_pre_market(autage=True)

    # test_sell()

    # test_reaching_target("SELL")
    # test_reaching_target("SELL", done_percent=50)
    # test_reaching_target("SELL", done_percent=50, confirm_buy = "BOOK")

    # test_reaching_target( "SL")
    # test_reaching_target("SL", done_percent=50)
    # test_reaching_target("SL", done_percent=50, confirm_buy="BOOK")

    # test_reaching_target("SL", done_percent= 50, cancel_on_EOM=True)
    # test_reaching_target("SELL", done_percent= 50, cancel_on_EOM=True)
    # test_reaching_target("SL", done_percent= 50, confirm_buy="BOOK", cancel_on_EOM=True)
    # test_reaching_target("SELL", done_percent= 50, confirm_buy="BOOK", cancel_on_EOM=True)

if __name__ == "__main__":
    test()

