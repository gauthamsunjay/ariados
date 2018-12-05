import boto3
import json
import os

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
        payload = json.dumps({"url": url})
        result = self.invoke("handle_single_url", payload)
        jresult = json.loads(result["Payload"].read().decode("utf-8"))
        return jresult

    def handle_multiple_urls(self, urls):
        """
        returns a list of dictionaries, each having a success flag along with
        idx, url. data and links are included if success is True, otherwise 'error'
        """
        payload = json.dumps({"urls": urls})
        result = self.invoke("handle_multiple_urls", payload)
        jresult = json.loads(result["Payload"].read().decode("utf-8"))
        if not jresult['success']:
            # if not a success, something wrong with the lambda itself.
            # TODO log this
            raise Exception("Got bad response %r" % jresult)

        return jresult['result']
