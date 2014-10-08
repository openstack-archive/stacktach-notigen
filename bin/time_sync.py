import datetime

import falcon


# gunicorn --log-file=- 'time_sync:get_api()'
#
# To get the current time
# curl localhost:8000/time
#
# To set the current time
# curl --data "2014-10-09 22:55:33.111111" localhost:8000/time

class TimeResource(object):
    def __init__(self):
        self.last_time = None

    def on_get(self, req, resp):
        resp.body = self.last_time
        print "GET", self.last_time

    def on_post(self, req, resp):
        chunk = req.stream.read(4096)
        self.last_time = chunk
        print "POST", self.last_time


api = falcon.API()
time_resource = TimeResource()
api.add_route('/time', time_resource)


def get_api():
    return api
