#  TODO: considerar los cambios de fecha durante la ejecución. Esto implicaría algún tipo de reset
#  TODO: considerar los eventuales nominales que accidentalemente quedaron comprados del día anterior
from marketmanager import MarketManager
import logging

import unittest.mock as mock
import datetime

# TODO : dos errores a tener en cuenta:
#  excepción en main: ServerException(response['Error']['Descripcion'] or 'Unknown Error')
#  pyhomebroker.common.exceptions.ServerException: Ocurrió un error inesperado. Por favor intente nuevamente más tarde - (20220217144533)
#  ERROR - on_error - * Homebroker * NoLogin - Connection_lost: False - Connected: True
#  on_error - * Homebroker * (SSLEOFError(8, 'EOF occurred in violation of protocol (_ssl.c:2384)'),) - Connection_lost: True - Connected: True
#         ERROR - reconnect - During reconnection: NoLogin

if __name__ == '__main__':
    mm = MarketManager(timer_off = True)
    # mm.connect()

    # TODO: Hay que manejar las excepciones y dar alerta (mínimo conexión y login).
    #  el status de conexión y login de pyhomebroker no son confiables, siguen dando true aún desconectados
    cmd = ""
    while cmd != "exit":
        if cmd == "stop":
            mm.timer.stop()
        elif cmd == "start":
            mm.timer.start()
        elif cmd == "connect":
            mm.connect()
        elif cmd == "connected":
            print(f"Connected = {mm.hb.is_connected()}")
        elif cmd == "disconnect":
            mm.disconnect()
        elif cmd == "reset":
            logging.info("Restarting ...")
            mm.timer.stop()
            mm.disconnect()
            del(mm)
            mm = MarketManager()
            mm.connect()
        elif cmd == "ostatus":
            print(mm.hb.get_orders_status())
        elif cmd == "status":
            print(mm.status_report())
        elif cmd == "forcepre":
            with mock.patch("helpers.get_time") as gt:
                gt.return_value = datetime.time(10, 55, 00)
                mm.on_timer_tick()
        elif cmd == "forcepost":
            mm.timer.stop()
            with mock.patch("helpers.get_time") as gt:
                gt.return_value = datetime.time(16, 55, 00)
                mm.on_timer_tick()
            mm.timer.start()

        # TODO: tratar de meter instrucciones python para evaluar

        cmd = input( "Command:" )

    logging.info("Exiting ...")

    if mm.timer.status() != "stopped":
        mm.timer.stop()

    if mm.hb.is_connected():
        mm.disconnect()

    logging.shutdown()
