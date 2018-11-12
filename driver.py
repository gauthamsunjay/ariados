import argparse
import json
from aws import Function

def get_args():
    argparser = argparse.ArgumentParser()
    argparser.add_argument("url")
    return argparser.parse_args()

def main(args):
    url = args.url
    fn = Function()
    fn_name = fn.register(
        function_name="uwcs_parser",
        handler="uwcs_parser.run_parser",
        description="parser for uw computer science pages",
        zip_file="./uwcs_parser.zip"
    )
    resp = fn.invoke(
        function_name=fn_name,
        invocation_type="RequestResponse",
        payload=json.dumps({"url": url})
    )
    data = json.loads(resp["Payload"].read().decode("utf-8"))
    print json.dumps(data, indent=2)

if __name__ == "__main__":
    args = get_args()
    main(args)

