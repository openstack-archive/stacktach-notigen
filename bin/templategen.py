# Creates notification templates from a StackTach.v2 mysql database.
#
# (as always, best to do this in a venv)
#
# You'll need the mysql connector:
# pip install mysql-connector-python --allow-external mysql-connector-python
#
# pip install --pre notification_utils
# pip install dateutil

import datetime
import json
import re
import sys

import dateutil.parser
import mysql.connector
import notification_utils as nu

def get_all_events(cnx, date_range, next_range):
    # Get all the events (including EOD .exists) for a day

    query = ("SELECT stacktach_rawdata.when AS d, stacktach_rawdata.event as event, "
             "stacktach_rawdata.json AS rawjson "
             "FROM stacktach_rawdata "
             "WHERE (stacktach_rawdata.when BETWEEN %f AND %f) "
             "ORDER BY stacktach_rawdata.when" % date_range)
    cursor = cnx.cursor()
    cursor.execute(query)
    payloads = []
    for when, event, rawjson in cursor:
        hwhen = nu.dt_from_decimal(when)
        queue, full = json.loads(rawjson)
        payload = full['payload']
        if event == 'compute.instance.exists':
            astart = dateutil.parser.parse(payload['audit_period_beginning'])
            aend = dateutil.parser.parse(payload['audit_period_ending'])

            # Ignore these EOD events, they're in tomorrow's batch.
            if astart.time() == datetime.time.min and aend.time() == datetime.time.min:
                # print "EOD .exists at: %s (%s -> %s)" % (hwhen, astart, aend)
                continue
        payloads.append((hwhen, full))
    cursor.close()

    query = ("SELECT stacktach_rawdata.when AS d, stacktach_rawdata.json AS rawjson "
             "FROM stacktach_rawdata "
             "WHERE (stacktach_rawdata.when BETWEEN %f AND %f) "
             "AND stacktach_rawdata.event='compute.instance.exists' "
             "ORDER BY stacktach_rawdata.when" % next_range)
    cursor = cnx.cursor()
    cursor.execute(query)
    for when, rawjson in cursor:
        hwhen = nu.dt_from_decimal(when)
        queue, full = json.loads(rawjson)
        payload = full['payload']
        astart = dateutil.parser.parse(payload['audit_period_beginning'])
        aend = dateutil.parser.parse(payload['audit_period_ending'])

        # Ignore these EOD events, they're in tomorrow's batch.
        if astart.time() != datetime.time.min or aend.time() != datetime.time.min:
            # print "Instant .exists at: %s (%s -> %s)" % (hwhen, astart, aend)
            continue
        payloads.append((hwhen, full))
    cursor.close()

    print "%d events for time period" % (len(payloads), )
    return payloads


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

print "Events from %s to %s" % (oldest, newest)

start_of_full_day = oldest.replace(hour=0, minute=0, second=0, microsecond=0)
end_of_full_day = oldest.replace(hour=23, minute=59, second=59, microsecond=999999)

start_of_full_day = start_of_full_day + datetime.timedelta(days=1)
end_of_full_day = end_of_full_day + datetime.timedelta(days=1)
start_of_next_day = start_of_full_day + datetime.timedelta(days=1)
end_of_next_day = end_of_full_day + datetime.timedelta(days=1)

print "Extracting from %s to %s" % (start_of_full_day, end_of_full_day)

if end_of_full_day > newest:
    print "Not enough events to get a full day."
    sys.exit(1)

date_range = (nu.dt_to_decimal(start_of_full_day),
              nu.dt_to_decimal(end_of_full_day))

next_range = (nu.dt_to_decimal(start_of_next_day),
              nu.dt_to_decimal(end_of_next_day))

query = ("SELECT DISTINCT(stacktach_rawdata.request_id) AS req "
         "FROM stacktach_rawdata "
         "WHERE stacktach_rawdata.when BETWEEN %f AND %f" % date_range)
cursor = cnx.cursor()
cursor.execute(query)
req = [r[0] for r in cursor]
cursor.close()

print "%d unique requests" % len(req)

query = ("SELECT DISTINCT(stacktach_rawdata.event) AS event "
         "FROM stacktach_rawdata "
         "WHERE stacktach_rawdata.when BETWEEN %f AND %f" % date_range)
cursor = cnx.cursor()
cursor.execute(query)
events = [event[0] for event in cursor]
cursor.close()

print "%d unique events" % len(events)

operations = [
    'compute.instance.shutdown.start',
    'compute.instance.delete.start',
    'compute.instance.snapshot.start',
    'compute.instance.create.start',
    'compute.instance.reboot.start',
    'compute.instance.rebuild.start',
    'compute.instance.resize.start',
    'compute.instance.finish_resize.start',
    'compute.instance.resize.confirm.start',
    'compute.instance.resize.prep.start',
    'compute.instance.power_off.start',
    'compute.instance.rescue.start',
    'compute.instance.unrescue.start',
]

patterns = {}  # {stream_type: {stream_length: first stream}}
for r in req:
    query = ("SELECT instance, stacktach_rawdata.when AS generated, "
             " event, json "
             "FROM stacktach_rawdata "
             "WHERE request_id='%s'" % (r, ))
    cursor = cnx.cursor()
    cursor.execute(query)
    stream = []
    stream_type = None
    for uuid, when, event, rawjson in cursor:
        if event in operations:
            if not stream_type:
                stream_type = event
        queue, full = json.loads(rawjson)
        stream.append((when, uuid, event, full))
    cursor.close()
    if not stream_type:
        continue
    pattern = patterns.get(stream_type, {})
    length = len(stream)
    if pattern.get(length) is not None:
        continue  # seen this before ...
    pattern[length] = stream
    patterns[stream_type] = pattern


def placeholder(value, original, context, key, template, parent):
    # value = "this is some [[[[FOO_%d]]]] string"
    # original = "1.2.3.4"
    # template = "[[[[FOO_%d]]]]"
    # key = "ip"
    #
    # Context = { key : { original: "[[[[FOO_?]]]]" }}
    mappings = context.get(key, {})
    index = mappings.get(original)
    if index is None:
        index = len(mappings)
        mappings[original] = index
        context[key] = mappings
    value = value % index
    parent(value)
    return value


def _replace_list(data, index):
    def _inner(value):
        data[index] = value
    return _inner


def _replace_dict_value(data, key):
    def _inner(value):
        data[key] = value
    return _inner


def _replace_dict_key(data, value, old_key):
    def _inner(key):
        del data[old_key]
        data[key] = value
    return _inner


uuid_regex = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                        re.IGNORECASE)
xuuid_regex = re.compile(r"[0-9a-f]{32}", re.IGNORECASE)
v4_regex = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}")
v6_regex = re.compile("([0-9a-f]{1,4}:){7}[0-9a-f]{1,4}", re.IGNORECASE)
dt_regex = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}"
                      r".[0-9]{2}[:-][0-9]{2}[:-][0-9]{2}\.[0-9]+")
dt1_regex = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}"
                      r".[0-9]{2}[:-][0-9]{2}[:-][0-9]{2}")
dt2_regex = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}"
                      r".[0-9]{2}[:-][0-9]{2}")
regex_list = [(uuid_regex, "uuid", "[[[[UUID_%d]]]]"),
              (xuuid_regex, "xuuid", "[[[[XUUID_%d]]]]"),
              (v4_regex, "v4", "[[[[V4_%d]]]]"),
              (v6_regex, "v6", "[[[[V6_%d]]]]"),
              (dt_regex, "dt", "[[[[DT_%d]]]]"),
              (dt1_regex, "dt", "[[[[DT_%d]]]]"),
              (dt2_regex, "dt", "[[[[DT_%d]]]]"),
              ]


def scrub(context, struct, parent):
    #print struct, type(struct)
    if type(struct) is list:
        #print "List"
        for index, x in enumerate(struct):
            scrub(context, x, _replace_list(struct, index))
    elif type(struct) is dict:
        #print "Dict"
        for k, v in struct.iteritems():
            scrub(context, v, _replace_dict_value(struct, k))
            scrub(context, k, _replace_dict_key(struct, v, k))
    elif type(struct) in [unicode, str]:
        #print "Str:", struct
        for regex, pkey, ptemplate in regex_list:
            m = regex.search(struct)
            if m:
                source = struct
                original = struct[m.start():m.end()]
                full_template = struct[:m.start()] + ptemplate + struct[m.end():]
                print "Found %s:" % pkey, source, original, struct
                struct = placeholder(full_template, original, context, pkey, ptemplate, parent)


for stream_type, streams_by_len in patterns.iteritems():
    for length, stream in streams_by_len.iteritems():
        context = {}
        for when, uuid, event, rawjson in stream:
            scrub(context, rawjson, None)
            print json.dumps(rawjson, sort_keys=True, indent=4)
            sys.exit(1)

cnx.close()
