import logging
import traceback
import json

import tornado.web
import tornado.ioloop

from .master import Master
logger = logging.getLogger(__name__)

class MasterHandler(tornado.web.RequestHandler):
    def initialize(self, master_fns):
        self.master_fns = master_fns

    def post(self, path):
        data = {}
        try:
            data = json.loads(self.request.body)
        except:
            pass

        fn = self.master_fns.get(path)
        if fn is None:
            self.write(json.dumps({"status": -1, "msg": "%r is not a valid function" % path}))
            return

        try:
            response = fn(**data)
            self.write(json.dumps({"status": 0, "result": response}))
        except Exception as e:
            logger.error("Got error for function", exc_info=True)
            self.write(json.dumps({"status": -1, "msg": repr(e), "traceback": traceback.format_exc()}))


def run_server(host, port, master):
    assert isinstance(master, Master), "expected Master found %r" % type(master)

    master_functions = master.get_http_endpoints()

    handlers = [
            (r'/(.*)', MasterHandler, {"master_fns": master_functions}),
    ]

    logger.info("starting...")
    app = tornado.web.Application(handlers)
    app.listen(port)
    tornado.ioloop.IOLoop.instance().start()
    logger.info("tornado stopped..")
