# Config file
import datetime
import logging
import __main__

# time lapse in seconds for the timer tick
timer_tick = 60

pre_market_start = datetime.time(hour=10, minute = 55)
market_start = datetime.time(hour=11, minute = 0)
pre_market_end = datetime.time(hour=16, minute = 50)
market_end = datetime.time(hour=17, minute = 0)
post_market_end = datetime.time(hour=17, minute = 5)

cancel_bt = datetime.time(hour=16, minute = 0)

asset_strategies = {"ARCO_1" : {"ticker": "ARCO", "class": "Strategy1", "buy_threshold": .985, "sl_threshold": .97, "win_target": 1.02, "nominals_target": 4, "stop_buy_time": cancel_bt},
                    "FB_1" : {"ticker": "META", "class": "Strategy1", "buy_threshold": .99, "sl_threshold": .975, "win_target": 1.01, "nominals_target": 1, "stop_buy_time": cancel_bt}
                   }

# - DANGER - mode "SIM"/"REAL" indica si las órdenes se simulan o si efectivamente se envían al broker
mode = "SIM"

# Si la ejecución arrancó con main.py logea a archivo, sino (test) a consola
if __main__.__file__.find("main.py") >= 0:
    log_output = "Logs\\" + datetime.datetime.today().strftime("%Y%m%d") + ".log"
else:
    log_output = None
logging.basicConfig(filename=log_output, style="{", format="{asctime} - {levelname} - {funcName} - {message}", level="INFO")
