# Copyright (c) 2014 Dark Secret Software Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""OpenStack-like Notification Generation Library

Originally built from work done in
https://github.com/SandyWalsh/twobillion
"""

import datetime
import heapq
import json
import operator
import os
import os.path
import uuid as uuidlib
import random
import sys
import time
import uuid

import dateutil.parser


class EventGenerator(object):
    def __init__(self, template_dir, operations_per_hour=1, exists_hours=24):
        self.exists_hours = exists_hours  # num hours between .exists
        self.instances = {}  # { uuid: compute_node }

        # Many actions can be performed concurrently.
        # We might start working on instance #1 and then, while that
        # effort is still underway, start doing something with instances
        # #2, 3 and 4. They need to interleave each other.
        #
        # An "action", below, is a list of each of the steps necessary
        # to perform that operation, but with a time component relative to
        # the starting time passed in.
        # It's our responsibility to fire off the events when sufficient "time"
        # has passed.
        #
        # The thing we don't want to have to deal with is overlapping commands
        # (like instance.delete starting while instance.create is still underway)
        # That's too much headache.

        operations_per_second = float(operations_per_hour) / 3600.0

        # An operation will happen every so many milliseconds to
        # get our operations/sec. We call this a Tick.
        self.millisecond_per_tick = 1000.0 / float(operations_per_second)
        #print "Operation every %d ms (%.1f/sec)" % (self.millisecond_per_tick,
        #                                          operations_per_second)
        self.next_events = []  # priority queue
        self.instances_in_use = set()

        now = datetime.datetime.utcnow()
        self.tick = now + datetime.timedelta(
                                    milliseconds=self.millisecond_per_tick)

        # When were last .exists sent?
        self.last_exists = now.replace(hour=0, minute=0, second=0, microsecond=0)

        # Load all the templates ...
        template_filenames = [f for f in os.listdir(template_dir)
                              if os.path.isfile(os.path.join(
                                               template_dir, f)) and
                                 f[-5:] == ".json"]

        self.templates = []
        for filename in template_filenames:
            with open(os.path.join(template_dir, filename), "r") as f:
                print filename
                template = json.load(f)
                # Keep it as a raw string to make replacements easier ...
                raw = json.dumps(template[1:], sort_keys=True, indent=4)
                if filename == "eod_exists.json":
                    self.exists_template = (template[0], raw)
                else:
                    operation = filename[:filename.index('_')]
                    self.templates.append((operation, template[0], raw))


    def move_to_next_tick(self, now):
        return now + datetime.timedelta(milliseconds=self.millisecond_per_tick)

    def generate(self, now):
        self._add_new_sequence(now)
        return self._get_ready_events(now)

    def _add_new_sequence(self, now):
        """Add a new operation to the queue.
        This is the entire sequence of events going into
        the future. They will be interwoven with other
        future events and pumped out in proper (interleaving)
        order."""
        if now >= self.tick:
            context, sequence = self._get_sequence(now)
            for idx, when_event in enumerate(sequence):
                when, event = when_event
                event['____context____'] = context  # delete before returning
                # (when, is_first_event, is_last_event)
                heapq.heappush(self.next_events,
                                    (when, event, idx==0, idx==len(sequence)-1))
            self.tick = self.move_to_next_tick(now)
        return now

    def _get_ready_events(self, now):
        """Pump out the events that are due now."""
        ready = []
        while True:
            if not self.next_events:
                return ready
            when, event, start, end = self.next_events[0]  # peek
            if when > now:
                break
            when, event, start, end = heapq.heappop(self.next_events)
            context = event['____context____']
            operation = context['operation']
            uuid = context['instance_id']
            if end:
                if operation == 'compute.instance.delete.start':
                    try:
                        self.instances_in_use.remove(uuid)
                    except KeyError:
                        pass
                else:
                    self.instances_in_use.add(uuid)

            del event['____context____']
            ready.append(event)

        if (now - self.last_exists).days > 0:
            flattened = now.replace(hour=0, minute=0, second=0, microsecond=0)

            self.last_exists = flattened

            audit_period_start = flattened - datetime.timedelta(days=1)
            audit_period_end = flattened
            operation = "eod-exists"
            context_hints, template = self.exists_template
            for instance in self.instances_in_use:
                context, sequence = self._make_sequence_from_template(now,
                                        operation, context_hints, template)
                for when, event in sequence:
                    payload = event['payload']
                    payload['audit_period_beginning'] = str(audit_period_start)
                    payload['audit_period_ending'] = str(audit_period_end)
                    payload['instance_id'] = instance
                    ready.append(event)
            self.instances_in_use = set()
        return ready

    def _get_sequence(self, now):
        """Grab a template and make a sequence from it.
        """

        operation, context_hints, template = random.choice(self.templates)
        return self._make_sequence_from_template(now, operation,
                                                 context_hints, template)

    def _make_sequence_from_template(self, now, operation, context_hints,
                                     template):
        sequence = []
        context = {}
        time_map = context_hints['time_map']
        for key, values in time_map.iteritems():
            context[key] = str(now + datetime.timedelta(days=values[0],
                                                    seconds=values[1],
                                                    milliseconds=values[2]))
        for num in range(context_hints['uuid']):
            context["[[[[UUID_%d]]]]" % num] = str(uuid.uuid4())

        for num in range(context_hints['xuuid']):
            u = str(uuid.uuid4()).replace("-", "")
            context["[[[[XUUID_%d]]]]" % num] = u

        for num in range(context_hints['v4']):
            nums = [127 + random.randrange(127) for x in range(4)]
            v4 = "%d.%d.%d.%d" % tuple(nums)
            context["[[[[V4_%d]]]]" % num] = v4

        for num in range(context_hints['v6']):
            res = [hex(random.randint(0, 65535))[2:].zfill(4)
                        for i in range(0, 8)]
            v6 =  ":".join(res)
            context["[[[[V6_%d]]]]" % num] = v6

        # The rest of the context ...
        context["[[[[tenant_id]]]]"] = str(100000 + random.randrange(899999))
        context["[[[[user_id]]]]"] = str(100000 + random.randrange(899999))
        context["[[[[display_name]]]]"] = "Instance_%d" % random.randrange(
                                                                899999)
        context["[[[[host]]]]"] = "host-%d" % random.randrange(899999)
        context["[[[[hostname]]]]"] = "server-%d" % random.randrange(899999)
        context["[[[[node]]]]"] = "node-%d" % random.randrange(899999)
        context["[[[[reservation_id]]]]"] = "res-%d" % random.randrange(899999)
        context["[[[[image_name]]]]"] = "image-%d" % random.randrange(899999)
        context["[[[[device_name]]]]"] = "device-%d" % random.randrange(899999)
        context["[[[[publisher_id]]]]"] = "publisher-%d" % random.randrange(
                                                                899999)

        for key, value in context.iteritems():
            template = template.replace(key, value)

        struct = json.loads(template)

        instance_id = None
        for event in struct:
            inst_id = event['payload'].get('instance_id')
            if inst_id:
                if instance_id and instance_id != inst_id:
                    print "changing instance id", instance_id, inst_id
                instance_id = inst_id
            when = dateutil.parser.parse(event['timestamp'])
            sequence.append((when, event))

        context['instance_id'] = instance_id
        context['operation'] = operation
        return context, sorted(sequence)


if __name__ == '__main__':

    real_time = False

    g = EventGenerator("templates", 1)
    now = datetime.datetime.utcnow()
    print "starting at", now
    end = now + datetime.timedelta(days=1)  # ensure .exists get generated
    start = now
    nevents = 0
    while now < end:
        e = g.generate(now)
        if e:
            nevents += len(e)

        if real_time:
            now = datetime.datetime.utcnow()
        else:
            now = g.move_to_next_tick(now)

    print "Elapsed: ", datetime.datetime.utcnow() - start
