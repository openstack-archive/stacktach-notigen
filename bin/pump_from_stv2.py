"""Pump simulated OpenStack notificationss into RabbitMQ.

You need to install rabbitqm-server and
pip install librabbitmq
pip install --pre notabene
pip install --pre notification_utils
"""


import datetime
import json
import sys
import time

import mysql.connector
from notabene import kombu_driver as driver
import notification_utils as nu


connection = driver.create_connection("localhost", 5672, 'guest', 'guest',
                                      "librabbitmq", "/")
exchange = driver.create_exchange("monitor", "topic")
queue_name = "monitor.info"
queue = driver.create_queue(queue_name, exchange, queue_name,
                            channel=connection.channel())
queue.declare()

cnx = mysql.connector.connect(user='root', password='password',
                              host='127.0.0.1',
                              database='stacktach')

cursor = cnx.cursor()
query = ("SELECT min(stacktach_rawdata.when) AS mindate, "
         "MAX(stacktach_rawdata.when) AS maxdate "
         "FROM stacktach_rawdata")
cursor.execute(query)
mindate, maxdate = list(cursor)[0]
cursor.close()

oldest = nu.dt_from_decimal(mindate)
newest = nu.dt_from_decimal(maxdate)

# Move to the next day so we get all the events for the day.
start = oldest.replace(hour=0,minute=0,second=0,microsecond=0) \
                                            + datetime.timedelta(days=1)
end = start + datetime.timedelta(days=1, hours=4)

print "Events from %s to %s" % (oldest, newest)
print "Extracting events from %s to %s" % (start, end)

time.sleep(5)

dstart = nu.dt_to_decimal(start)
dend = nu.dt_to_decimal(end)

cursor = cnx.cursor()
query = ("SELECT stacktach_rawdata.when AS d, "
         "stacktach_rawdata.json AS rawjson "
         "FROM stacktach_rawdata "
         "WHERE (stacktach_rawdata.when BETWEEN %f AND %f) "
         "AND stacktach_rawdata.event!='compute.instance.updates' "
         "AND stacktach_rawdata.event!='compute.instance.exists.verified' "
         "ORDER BY stacktach_rawdata.when LIMIT 100000" % (dstart, dend))
cursor.execute(query)

start = None
end = None
num = 0
for when, rawjson in cursor:
    when = nu.dt_from_decimal(when)
    if not start:
        start = when
        end = start + datetime.timedelta(days=1)
    if when > end:
        break
    queue, event = json.loads(rawjson)
    # Skip the noise ...
    if event['event_type'] in ['compute.instance.update', 'compute.instance.exists.verified']:
        continue
    print when, event['event_type']
    driver.send_notification(event, queue_name, connection, exchange)
    num+=1

print "Published %d events" % num

cursor.close()
cnx.close()
