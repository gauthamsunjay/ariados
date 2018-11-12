import boto3
import json
import os

aws_profile = os.environ.get("AWS_PROFILE", "gsunjay")

# TODO: change role to only lambda service
policy_document = {
      "Version": "2012-10-17",
      "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                    # "Service": "lambda.amazonaws.com"
                # },
                "Action": "sts:AssumeRole"
            }
      ]
}


class Function(object):
    def __init__(self, region="us-west-2"):
        session = boto3.Session(profile_name=aws_profile, region_name=region)
        self.lambda_client = session.client("lambda")
        self.iam_client = session.client("iam")
        self.credentials_arn = None

    def __create_role(self):
        resp = self.iam_client.create_role(
            RoleName="LambdaFunctionExecutionRole",
            AssumeRolePolicyDocument=json.dumps(policy_document)
        )
        self.credentials_arn = resp["Role"]["Arn"]

    def register(self,
                 function_name=None,
                 handler=None,
                 description=None,
                 timeout=30,
                 memory_size=512,
                 environment_variables=None,
                 dead_letter_config=None,
                 publish=True,
                 zip_file=None,
                 kms_key_arn=None,
                 vpc_config=None,
                 tracing_config=None,
                 tags=None,
                 runtime='python2.7'
                 ):
        """
        Wrapper around aws lambda client
        """
        if not zip_file or not os.path.exists(zip_file):
            raise Exception("Zip file %s not passed or does not exist." %
                            zip_file)

        if not environment_variables:
            environment_variables = {}
        if not dead_letter_config:
            dead_letter_config = {}
        if not kms_key_arn:
            kms_key_arn = ""
        if not vpc_config:
            vpc_config = {}
        if not tracing_config:
            tracing_config = {}
        if not tags:
            tags = {}
        if not self.credentials_arn:
            self.credentials_arn = \
                "arn:aws:iam::665143496549:role/first_lambda_role"

        with open(zip_file, "rb") as zp:
            zip_file = zp.read()

        kwargs = dict(
            FunctionName=function_name,
            Runtime=runtime,
            Code={"ZipFile": zip_file},
            Role=self.credentials_arn,
            Handler=handler,
            Description=description,
            Timeout=timeout,
            MemorySize=memory_size,
            Publish=publish,
            VpcConfig=vpc_config,
            DeadLetterConfig=dead_letter_config,
            Environment={"Variables": environment_variables},
            KMSKeyArn=kms_key_arn,
            TracingConfig=tracing_config,
            Tags=tags,
        )

        # time.sleep(5)
        rsp = self.lambda_client.create_function(**kwargs)
        return rsp['FunctionArn']

    def invoke(self,
               function_name,
               payload,
               invocation_type,
               log_type='Tail',
               ):
        return self.lambda_client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            LogType=log_type,
            Payload=payload
        )


if __name__ == "__main__":
    fn = Function()
    fn_name = fn.register(
        function_name="add_fn",
        handler="add_fn.add",
        description="simple add function",
        zip_file="./playground/add.zip"
    )
    resp = fn.invoke(
        function_name=fn_name,
        invocation_type="RequestResponse",
        payload=json.dumps({"a": 2, "b": 3})
    )
    print json.loads(resp["Payload"].read().decode("utf-8"))
