import boto3
import json
import os
import time

from botocore.exceptions import ReadTimeoutError

from ariados.common import stats

class Invoker(object):
    def __init__(self):
        pass

    def handle_single_url(self, url):
        raise NotImplementedError

    def handle_multiple_urls(self, urls):
        raise NotImplementedError

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
            if not jresult['success']:
                # if not a success, something wrong with the lambda itself.
                # TODO log this
                raise Exception("Got bad response %r" % jresult)

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
