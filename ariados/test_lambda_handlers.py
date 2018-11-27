import json
import sys

from ariados.lambda_handlers import handle_single_url, handle_multiple_urls

def test_handle_single_url(url):
    event = {'url':url}
    data, links = handle_single_url(event, None)
    print json.dumps({"data": data, "links": links}, indent=2)

def test_handle_multiple_urls(urls):
    event = {'urls': urls}
    output = handle_multiple_urls(event, None)
    print json.dumps(output, indent=2)

if __name__ == '__main__':
    urls = [
        "https://www.cs.wisc.edu/calendar/month/2018-09",
        "https://www.cs.wisc.edu/calendar/month/2018-08",
        "https://www.cs.wisc.edu/calendar/month/2018-07",
        "https://www.cs.wisc.edu/calendar/month/2018-06",
        "https://www.cs.wisc.edu/events/3983",
        "https://www.cs.wisc.edu/events/3984",
        "https://www.cs.wisc.edu/events/4000",
        "https://www.cs.wisc.edu/events/4005",
        "https://www.cs.wisc.edu/events/4006",
        # "https://www.cs.wisc.edu/events/3946", # <--- fails
    ]

    if len(sys.argv) > 1:
        urls = sys.argv[1:]

    if len(urls) == 1:
        test_handle_single_url(urls[0])
    else:
        test_handle_multiple_urls(urls)
