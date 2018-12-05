#!/usr/bin/env python

import argparse

from statsd import StatsClient

from .server import run_server
from .master import Master

from ariados.common import stats

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument("--port", default=8888, type=int)
    # TODO add args for influxdb, store, other stuff
    # TODO add log level
    return parser.parse_args()

def main(args):
    stats.set_default_client(StatsClient(host="localhost", port=8125))
    run_server(args.host, args.port, Master())

if __name__ == '__main__':
    args = get_args()
    main(args)
