import traceback

import flask

def handle_single_url_cb(result):
    print "handle_single_url: got result %r" % result

def handle_multiple_url_cb(result):
    print "handle_multiple_url_cb: got result %r" % result

def handle_async_error_cb(urls):
    print "failed for these urls: %s" % urls

def run_server(host, port):

    master_functions = {
        "cb/handle_single_url": handle_single_url_cb,
        "cb/handle_multiple_urls": handle_multiple_url_cb,
        "cb/handle_multiple_urls/error": handle_async_error_cb,
    }

    app = flask.Flask(__name__)
    @app.route('/', defaults={'path': ''}, methods=("GET", "POST"))
    @app.route('/<path:path>', methods=("GET", "POST"))
    def catch_all(path):
        fn = master_functions.get(path)
        if fn is None:
            return flask.jsonify({'status': -1, 'msg': '%r is not a valid function' % path})

        print "data is %r" % flask.request.get_data()
        data = {}
        if flask.request.method == "POST":
            try:
                data = flask.request.get_json(force=True) or {}
            except:
                return flask.jsonify({'status': -1, 'msg': 'require valid json'})

        elif flask.request.method == "GET":
            data = flask.request.args or {}
            data = {k:v for k,v in data.iteritems()}
        else:
            return flask.jsonify({'status': -1, 'msg': 'Only POST or GET allowed'})

        try:
            response = fn(**data)
            return flask.jsonify({'status': 0, 'result': response})
        except Exception as e:
            return flask.jsonify({'status': -1, 'msg': repr(e), 'traceback': traceback.format_exc()})

    app.run(host=host, port=port)

run_server("localhost", 13001)
