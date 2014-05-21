"""Pump simulated OpenStack notificationss into RabbitMQ.

You need to install rabbitqm-server and
pip install librabbitmq
pip install --pre notabene
pip install --pre notification_utils
"""


import datetime

from notabene import kombu_driver as driver
import notification_utils
import notigen


connection = driver.create_connection("localhost", 5672, 'guest', 'guest',
                                      "librabbitmq", "/")
exchange = driver.create_exchange("monitor", "topic")

g = notigen.EventGenerator(100)  # Number of operations per minute
now = datetime.datetime.utcnow()
start = now
nevents = 0
while nevents < 10000:
    e = g.generate(now)
    if e:
        nevents += len(e)
        for event in e:
            driver.send_notification(event, "monitor.info", connection, exchange)

    now = datetime.datetime.utcnow()
