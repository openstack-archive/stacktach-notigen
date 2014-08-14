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


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            encoded_object = list(obj.timetuple())[0:7]
        elif isinstance(obj, datetime.timedelta):
            encoded_object = (obj.days, obj.seconds, obj.microseconds)
        else:
            encoded_object = json.JSONEncoder.default(self, obj)
        return encoded_object


def get_all_events(cnx, date_range, next_range):
    # Get all the events (including EOD .exists) for a day

    query = ("SELECT stacktach_rawdata.when AS d, "
             "stacktach_rawdata.event as event, "
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
            if (astart.time() == datetime.time.min and
                aend.time() == datetime.time.min):
                # print "EOD .exists at: %s (%s -> %s)" % (hwhen, astart, aend)
                continue
        payloads.append((hwhen, full))
    cursor.close()

    query = ("SELECT stacktach_rawdata.when AS d, "
             "stacktach_rawdata.json AS rawjson "
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
        if (astart.time() != datetime.time.min or
            aend.time() != datetime.time.min):
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
end_of_full_day = oldest.replace(hour=23, minute=59, second=59,
                                 microsecond=999999)

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

patterns = {}  # {stream_type: {stream_length: first stream seen like this}}
for r in req:
    query = ("SELECT instance, stacktach_rawdata.when AS generated, "
             " event, json "
             "FROM stacktach_rawdata "
             "WHERE request_id='%s' "
             "ORDER BY stacktach_rawdata.when" % (r, ))

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
        continue  # seen one like this before ...
    pattern[length] = stream
    patterns[stream_type] = pattern


def placeholder(value, original, context, key, template, parent):
    # value = "this is some [[[[FOO_%d]]]] string"
    # original = "1.2.3.4"
    # template = "[[[[FOO_%d]]]]"
    # key = "ip"
    #
    # Context = { key : { original: num }}
    mappings = context.get(key, {})
    index = mappings.get(original)
    if index is None:
        index = len(mappings)
        mappings[original] = index
        context[key] = mappings
    value = value % index
    parent(value)
    return value, index


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


uuid_regex = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                        r"[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
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

# Many critical keys come under many similar names. None of which
# we want in the template.

# { common_name : list of aliases }
protect = {"tenant_id": ["_context_project_id", "_context_project_name",
                         "_context_tenant", "tenant_id", "project_id",
                         "project_name"],
           "user_id": ["_context_user", "_context_user_id",
                        "_context_user_name", "user_id", "owner",
                        "user_name", "image_user_id"],
           "display_name": ["display_name", "name", "display_description"],
           "host": ["host",],
           "hostname": ["hostname",],
           "node": ["node",],
           "reservation_id": ["reservation_id",],
           "image_name": ["image_name", ],
           "device_name": ["devname", ],
           "publisher_id": ["publisher_id",]}


def scrub(context, struct, parent):
    #print struct, type(struct)
    if type(struct) is list:
        #print "List"
        to_delete = []
        for index, x in enumerate(struct):
            scrub(context, x, _replace_list(struct, index))
            if type(x) in [unicode, str]:
                if ('RAX' in x or 'rack' in x or 'rax' in x
                    or 'rackspace' in x):
                        # reverse insert order so we can safely delete later.
                        to_delete.insert(0, index)

        for index in to_delete:
            struct.pop(index)

    elif type(struct) is dict:
        #print "Dict"
        to_delete = []
        for k, v in struct.iteritems():
            if ('service_catalog' in k or 'connection_info' in k
                or '_context_service_catalog' in k
                or 'RAX' in k or 'rack' in k
                or "password_info" in k
                or "rax" in k or "rackspace" in k):
                    to_delete.append(k)
                    continue
            for watch_key, watch_list in protect.iteritems():
                if k in watch_list:
                    struct[k] = "[[[[%s]]]]" % watch_key
                    continue
            scrub(context, v, _replace_dict_value(struct, k))
            scrub(context, k, _replace_dict_key(struct, v, k))
        for k in to_delete:
            del struct[k]

    elif type(struct) in [unicode, str]:
        #print "Str:", struct
        for regex, pkey, ptemplate in regex_list:
            m = regex.search(struct)
            if m:
                source = struct
                original = struct[m.start():m.end()]
                full_template = struct[:m.start()] + ptemplate \
                                + struct[m.end():]
                #print "Found %s:" % pkey, source
                struct, index = placeholder(full_template, original, context,
                                            pkey, ptemplate, parent)
                if pkey == 'dt':
                    start = context['_start_time']
                    time_map = context['_time_map']
                    now = dateutil.parser.parse(original)
                    time_map[ptemplate % index] = now - start

for stream_type, streams_by_len in patterns.iteritems():
    for length, stream in streams_by_len.iteritems():
        output = []
        context = {'_time_map': {}}
        for when, uuid, event, rawjson in stream:
            # All datetimes will be relative to the first event's timestamp.
            if not context.get('_start_time'):
                context['_start_time'] = dateutil.parser.parse(
                                                         rawjson['timestamp'])
            scrub(context, rawjson, None)
            output.append(rawjson)

        # output file is one large json array.
        # [0] = context hints, such as
        # {"time_map" : [
        #   {"[[[[[DT_0]]]]": (days, seconds, microseconds)}, time delta
        #   {"[[[[[DT_1]]]]": (days, seconds, microseconds)}, time delta
        #  ],
        #  "uuid": num   (number of unique UUID's needed)
        #  "xuuid": num   (number of unique UUID's need with no dividers)
        #  "v4": num    (number of IP.v4 addresses needed)
        #  "v6": num    (number of IP.v6 addresses needed)
        # }
        # [1..N] = stream of event templates for this operation
        # where N = the number in the filename foo.blah.NUM.json
        # ... that many events in this stream.
        context_map = {'time_map': context['_time_map']}
        for key in ['uuid', 'xuuid', 'v4', 'v6']:
            context_map[key] = len(context.get(key, []))
        output.insert(0, context_map)

        filename = "templates/%s_%d.json" % (stream_type, length)
        with open(filename, "w") as f:
            json.dump(output, f, cls=DateTimeEncoder, indent=4)

    if 0:
        timemap = output[0]['time_map']
        now = datetime.datetime.utcnow()
        this_context = {}
        for k, td in timemap.iteritems():
            this_context[k] = now + td
        for item in output[1:]:
            now.time().replace(0, 0, 0, 0)
            string = json.dumps(item, cls=DateTimeEncoder)
            for k, v in this_context.iteritems():
                string = string.replace(k, str(v))
            x = json.loads(string)
            print json.dumps(x, cls=DateTimeEncoder, sort_keys=True, indent=4)
            sys.exit(1)

query = ("SELECT json "
         "FROM stacktach_rawdata "
         "WHERE event='compute.instance.exists' AND "
         "      stacktach_rawdata.when BETWEEN %f AND %f " % date_range)

cursor = cnx.cursor()
cursor.execute(query)
got = False
for rawjson, in cursor:
    if got:
        continue
    queue, full = json.loads(rawjson)
    payload = full['payload']
    if full['event_type'] == 'compute.instance.exists':
        astart = dateutil.parser.parse(payload['audit_period_beginning'])
        aend = dateutil.parser.parse(payload['audit_period_ending'])
        if astart.time() != datetime.time.min:
            continue
        if aend.time() != datetime.time.min:
            continue

        context = {'_time_map': {},
                   '_start_time': dateutil.parser.parse(full['timestamp'])}
        scrub(context, full, None)

        context_map = {'time_map': context['_time_map']}
        for key in ['uuid', 'xuuid', 'v4', 'v6']:
            context_map[key] = len(context.get(key, []))

        output = [context_map, full]
        filename = "templates/eod_exists.json"
        with open(filename, "w") as f:
            json.dump(output, f, cls=DateTimeEncoder, indent=4)
        got = True
cursor.close()

cnx.close()
