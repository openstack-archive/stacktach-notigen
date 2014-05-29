"""The consumer part of event_pump. Uses Notabene to read
published notifications.

"""

import logging
import sys

from notabene import kombu_driver as driver


class Callback(object):
    x = 0
    def on_event(self, deployment, routing_key, body, exchange):
        print "Got:", body
        self.x += 1
        if self.x > 100000:
            sys.exit(1)

    def shutting_down(self):
        print "Shutting down"

config = {"topics":{
            "monitor":[
                {"queue":"monitor.info",
                 "routing_key":"monitor.info"},
            ]
         }}
logging.basicConfig(level=logging.DEBUG)
driver.start_worker(Callback(), "event_consumer", 1, config,
                 "monitor", logging)

