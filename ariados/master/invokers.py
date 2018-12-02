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
        payload = json.dumps({"url": url})
        return self.invoke("handle_single_url", payload)

    def handle_multiple_urls(self, urls):
        payload = json.dumps({"urls": urls})
        return self.invoke("handle_multiple_urls", payload)
