"""Pump simulated OpenStack notificationss into RabbitMQ.

You need to install rabbitqm-server and
pip install librabbitmq
pip install --pre notabene
pip install --pre notification_utils
"""


import datetime
import sys

from notabene import kombu_driver as driver
import notification_utils
import notigen


connection = driver.create_connection("localhost", 5672, 'guest', 'guest',
                                      "librabbitmq", "/")
exchange = driver.create_exchange("monitor", "topic")
queue_name = "monitor.info"
queue = driver.create_queue(queue_name, exchange, queue_name,
                            channel=connection.channel())
queue.declare()

print "Usage: python event_pump.py <template_dir> <operations/hour> " \
      "<realtime? 1/0>"
template_dir = sys.argv[1]
rate = int(sys.argv[2])
realtime = int(sys.argv[3]) == 1
print "Using template dir:", template_dir
print "Rate:", rate
print "Real-time?", realtime

g = notigen.EventGenerator(template_dir, rate)
now = datetime.datetime.utcnow()
start = now
end = now + datetime.timedelta(days=1)
nevents = 0
while now < end:
    e = g.generate(now)
    if e:
        nevents += len(e)
        for event in e:
            driver.send_notification(event, queue_name, connection, exchange)
            print event['timestamp'], event['event_type']

    if realtime:
        now = datetime.datetime.utcnow()
    else:
        now = g.move_to_next_tick(now)
