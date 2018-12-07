#!/usr/bin/env python

import argparse
import logging

from statsd import StatsClient

from .server import run_server
from .master import Master

from ariados.common import stats

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument("--port", default=8888, type=int)
    parser.add_argument("--stats-prefix", default="ariados")
    parser.add_argument("--log-level", default="info")
    # TODO add args for influxdb, store, other stuff
    # TODO add log level
    return parser.parse_args()

def main(args):
    format = "%(asctime)s %(levelname)s %(module)s:%(filename)s:%(lineno)s - %(funcName)20s() %(message)s"
    level = getattr(logging, args.log_level.upper())
    logging.basicConfig(format=format, level=level)
    assert isinstance(args.stats_prefix, basestring)
    stats.set_default_client(StatsClient(host="localhost", port=8125, prefix=args.stats_prefix))
    run_server(args.host, args.port, Master())

if __name__ == '__main__':
    args = get_args()
    main(args)
