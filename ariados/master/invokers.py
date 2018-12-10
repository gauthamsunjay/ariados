import boto3
import json
import logging
import os
import time

from threading import Condition

import requests
from botocore.exceptions import ReadTimeoutError

from ariados.common import stats
from ariados.common import constants

logger = logging.getLogger(__name__)

class Invoker(object):
    def __init__(self):
        pass

    def handle_single_url(self, url):
        raise NotImplementedError

    def handle_multiple_urls(self, urls):
        raise NotImplementedError

class AWSAsyncLambdaInvoker(Invoker):
    def __init__(self, async_server_addr):
        self.async_server_addr = async_server_addr
        self.lambda_free_cond = Condition()
        self.num_active_lambdas = 0
        self.url = "http://%s/multiple" % async_server_addr

    def handle_single_url(self, url):
        raise NotImplementedError

    def handle_multiple_urls(self, urls):
        stats.client.incr("lambdas.urls.processing", len(urls))
        self.lambda_free_cond.acquire()
        while self.num_active_lambdas >= constants.MAX_ACTIVE_LAMBDAS:
            self.lambda_free_cond.wait()

        self.num_active_lambdas += 1
        stats.client.gauge("lambdas.active", self.num_active_lambdas)
        self.lambda_free_cond.release()

        payload = {"urls":urls}
        resp = requests.post(self.url, data=json.dumps(payload))
        assert resp.status_code == 202, "expected status code 202 but found %r" % resp

    def got_response(self):
        self.lambda_free_cond.acquire()
        self.num_active_lambdas -= 1
        stats.client.gauge("lambdas.active", self.num_active_lambdas)
        self.lambda_free_cond.notify()
        self.lambda_free_cond.release()

class AWSLambdaInvoker(Invoker):
    def __init__(self):
        aws_profile = os.environ.get("AWS_PROFILE", "ariados")
        self.session = boto3.Session(profile_name=aws_profile)
        self.lambda_client = self.session.client("lambda")

    def invoke(self, function_name, payload, invocation_type="RequestResponse", log_type='Tail'):
        return self.lambda_client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            LogType=log_type,
            Payload=payload
        )

    def handle_single_url(self, url):
        """
        returns a single dictionary having a success flag, data and links if True,
        otherwise 'error'
        """
        stats.client.incr("lambdas.urls.processing")
        payload = json.dumps({"url": url})
        start = time.time()
        stat = "lambdas.invocation.success"
        try:
            result = self.invoke("handle_single_url", payload)
            jresult = json.loads(result["Payload"].read().decode("utf-8"))
            return jresult
        except Exception as ex:
            if isinstance(ex, ReadTimeoutError):
                stat = "lambdas.invocation.failure.timeout"
            else:
                stat = "lambdas.invocation.failure"
            raise
        finally:
            end = time.time()
            delta = (end - start) * 1000
            stats.client.timing(stat, delta)

    def handle_multiple_urls(self, urls):
        """
        returns a list of dictionaries, each having a success flag along with
        idx, url. data and links are included if success is True, otherwise 'error'
        """
        stats.client.incr("lambdas.urls.processing", len(urls))
        payload = json.dumps({"urls": urls})
        stat = "lambdas.invocation.success"
        start = time.time()
        try:
            result = self.invoke("handle_multiple_urls", payload)
            jresult = json.loads(result["Payload"].read().decode("utf-8"))
            success = jresult.get("success", None)
            if success in (None, False):
                logger.error("Got bad lambda response %r", jresult)
                raise Exception("Got bad lambda response %r", jresult)

            return jresult['result']
        except Exception as ex:
            if isinstance(ex, ReadTimeoutError):
                stat = "lambdas.invocation.failure.timeout"
            else:
                stat = "lambdas.invocation.failure"
            raise
        finally:
            end = time.time()
            delta = (end - start) * 1000
            stats.client.timing(stat, delta)
