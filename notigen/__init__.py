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

Built from work done in https://github.com/SandyWalsh/twobillion
"""

import datetime
import heapq
import uuid as uuidlib
import random
import time


COMPUTE_EVENTS = [
    'compute.instance.finish_resize.*',
    'compute.instance.power_off.*',
    'compute.instance.power_on.*',
    'compute.instance.reboot.*',
    'compute.instance.rebuild.*',
    'compute.instance.resize.confirm.*',
    'compute.instance.resize.prep.*',
    'compute.instance.resize.revert.*',
    'compute.instance.resize.*',
    'compute.instance.shutdown.*',
    'compute.instance.snapshot.*',
    'compute.instance.suspend',
    'compute.instance.resume',
    'compute.instance.exists',
    'compute.instance.update',
    'attach_volume',
    'change_instance_metadata',
    'detach_volume',
    'finish_resize',
    'finish_revert_resize',
    'get_vnc_console',
    'power_on_instance',
    'prep_resize',
    'reboot_instance',
    'rebuild_instance',
    'rescue_instance',
    'reserve_block_device_name',
    'resize_instance',
    'revert_resize',
    'run_instance',
    'set_admin_password',
    'snapshot_instance',
    'start_instance',
    'suspend_instance',
    'terminate_instance',
    'unrescue_instance']

SCHEDULER_EVENTS = ['scheduler.run_instance.start',
                    'scheduler.run_instance.scheduled',
                    'scheduler.run_instance.end']

SCHEDULERS = ['scheduler_%02d' % x for x in xrange(3)]

COMPUTE_NODES = ['compute_%03d' % x for x in xrange(100)]

API_NODES = ['api.server.%02d' % x for x in xrange(10)]


class EventGenerator(object):
    def __init__(self, operations_per_minute=1000):
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

        operations_per_second = float(operations_per_minute) / 60.0

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

    def generate(self, now):
        self._add_new_sequence(now)
        events = self._get_ready_events(now)
        for event in events:
            event['when'] = str(event['when'])
            event['message_id'] = str(uuidlib.uuid4())
        return events

    def move_to_next_tick(self, now):
        return now + datetime.timedelta(milliseconds=self.millisecond_per_tick)

    def _add_new_sequence(self, now):
        """Add a new operation to the queue.
        This is the entire sequence of events going into
        the future. They will be interwoven with other
        future events and pumped out in proper (interleaving)
        order."""
        if now >= self.tick:
            action = self._get_action(now)
            for idx, event in enumerate(action):
                when = event['when']
                if idx == 0:
                    uuid = event['uuid'][-4:]
                    request = event['request_id'][-4:]
                    if False:
                        if event['is_create']:
                            print "CREATE:",
                        if event['is_delete']:
                            print "DELETE:",
                        if event['is_update']:
                            print "UPDATE:",
                        print "U:%s R:%s" % (uuid, request),
                        print "(%d of %d)" % (len(self.instances_in_use), \
                              len(self.instances))
                # (when, event, is_first_event, is_last_event)
                heapq.heappush(self.next_events,
                                    (when, event, idx==0, idx==len(action)-1))
            self.tick = self.move_to_next_tick(now)
        return now

    def _get_ready_events(self, now):
        """Pump out all the ready events."""
        ready = []
        while True:
            if not self.next_events:
                return ready
            when, event, start, end = self.next_events[0]  # peek
            if when > now:
                return ready
            when, event, start, end = heapq.heappop(self.next_events)
            uuid = event['uuid']
            request = event['request_id']
            if end:
                if event['is_create']:
                    self.instances_in_use.add(uuid)
                elif event['is_delete']:
                    self.instances_in_use.remove(uuid)
            #print "%s %40s U:%4s" % (' ' * 20, event['event_type'], uuid[-4:])
            ready.append(event)

    def _get_action(self, now):
        """Get an action sequence. A series of related events
           that perform an operation. At this stage all it has
           is a request_id."""
        request_id = "req_" + str(uuidlib.uuid4())
        base = {'request_id': request_id}
        return self._make_action(now, base)

    def _make_action(self, now, base):
        """Start creating records that look like OpenStack events.

        api [-> scheduler] -> compute node.

        instances_in_use is different than instances.keys():
        instances.keys() is the list of all instances, even instances that
        don't exist yet, but will be created in the near future.
        instance_in_use are the instances in the current timeline.

        While there are no in-use instances, create new ones.

        After that, 10% chance of new instance. Otherwise,
        20% chance it's a delete. The remaining 80% are
        instance update operations.
        """
        event_chain = []

        is_create = random.randrange(100) < 10
        is_delete = False
        is_update = False

        uuid = str(uuidlib.uuid4())
        compute_node = random.choice(COMPUTE_NODES)

        if not is_create and not self.instances_in_use:
            is_create = True

        if not is_create:
            temp_uuid = random.choice(list(self.instances_in_use))
            try:
                compute_node = self.instances[temp_uuid]
                uuid = temp_uuid

                # 20% of the time it's a Delete, otherwise an Update ...
                is_delete = random.randrange(100) < 20
                if not is_delete:
                    is_update = True
            except KeyError:
                # The instance is in the process of being deleted.
                is_create = True

        if not (is_create or is_delete or is_update):
            raise Exception("Why?!")
            is_create = True

        nbase = {'uuid': uuid, 'is_create': is_create, 'is_delete': is_delete,
                 'is_update': is_update}
        nbase.update(base)

        # All operations start with an API call ...
        api = self._mk_event(now, nbase, API_NODES,
                       ['compute.instance.update'])
        event_chain.extend(api)

        if is_create:
            now = self._bump_time(now, 0.5, 3.0)  # From api to scheduler
            scheduler_node = random.choice(SCHEDULERS)
            for e in SCHEDULER_EVENTS:
                event_chain.extend(self._event(now, nbase, scheduler_node, e))
                now = self._bump_time(now, 0.1, 0.5)  # inside scheduler

            now = self._bump_time(now, 0.5, 3.0)  # In Compute node
            event_chain.extend(self._event(now, nbase, compute_node,
                                           'compute.instance.create.*'))
            self.instances[uuid] = compute_node

        if is_delete:
            event_chain.extend(self._event(now, nbase, compute_node,
                                           'compute.instance.delete.*'))
            del self.instances[uuid]

        if is_update:
            event = random.choice(COMPUTE_EVENTS)
            event_chain.extend(self._event(now, nbase, compute_node, event))

        return event_chain

    def _bump_time(self, now, low, high):
        """Create a random time in fractional seconds and move now ahead
        that amount."""
        secs = low + ((high - low) * random.random())
        return now + datetime.timedelta(seconds=secs)

    def _mk_event(self, now, base, nodes, events):
        """Make a single event with random node/events.
           If the event name ends in .* we will generate
           the corresponding .start and .end events
           while we're at it."""
        return self._event(now, base, random.choice(nodes),
                           random.choice(events))

    def _event(self, now, base, node, event):
        """Make a single event or a pair of events (depending on the
           event type)"""
        results = []
        if event[-1] == '*':
            event = event[0:-1]
            extra = {'when': now, 'node': node}
            results.append(self._pkg(base, extra, {'event_type': event + "start"}))
            now = self._bump_time(now, 0.25, 60.0 * 15.0)  # In compute node
            extra = {'when': now, 'node': node}
            results.append(self._pkg(base, extra, {'event_type': event + "end"}))
        else:
            extra = {'when': now, 'node': node}
            results.append(self._pkg(base, extra, {'event_type': event}))
        return results

    def _pkg(self, *args):
        """Pack together a bunch of dict's into a single dict."""
        new = {}
        for a in args:
            new.update(a)
        return new


if __name__ == '__main__':

    real_time = False

    g = EventGenerator(100)
    now = datetime.datetime.utcnow()
    start = now
    nevents = 0
    while nevents < 10000:
        e = g.generate(now)
        if e:
            nevents += len(e)

        if real_time:
            now = datetime.datetime.utcnow()
        else:
            now = g.move_to_next_tick(now)

    print "Elapsed: ", datetime.datetime.utcnow() - start
