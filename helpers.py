import logging
from threading import Timer
import datetime
import math

class BrokerTimer():
    """A Timer class that does not stop, unless you want it to."""

    def __init__(self, seconds, target):
        self._should_continue = False
        self.is_running = False
        self.seconds = seconds
        self.target = target
        self.thread = None
        self.run_on_start = True

    def _handle_target(self):
        self.is_running = True
        self.target()
        self.is_running = False
        self._start_timer()

    def _start_timer(self):
        if self._should_continue: # Code could have been running when cancel was called.
            self.thread = Timer(self.seconds, self._handle_target)
            self.thread.start()

    def start(self):
        if not self._should_continue and not self.is_running:
            self._should_continue = True
            if self.run_on_start:
                self._handle_target()
            else:
                self._start_timer()
        else:
            print("Timer already started or running, please wait if you're restarting.")

    def stop(self):
        if self.thread is not None:
            self._should_continue = False # Just in case thread is running and cancel fails.
            self.thread.cancel()
            self.thread = None
        else:
            logging.info("Timer not started or failed to initialize.")

    def status(self):
        if self.is_running:
            return "executing"
        elif self.thread:
            return "waiting"
        else:
            return "stopped"

def get_time():
    return datetime.datetime.now().time()


def round_price(price):
    """Redondea un precio en función del valor.
        valor < 100: queda como está, redondeando a 2 dígitos
        100 < valor <= 250: se redondea hacia abajo al mas cercano entre .0, .25, .5, .75
        valor > 250: se redondea a entero"""
    if price > 250:
        price = round(price)
    elif 100 < price <= 250:
        price = math.floor(price / .25) * .25
    else:
        price = round(price, 2)

    return price