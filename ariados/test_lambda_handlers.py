import sys

from ariados.lambda_handlers import handler

def test_handler(url):
    event = {'url':url}
    data, links = handler(event, None)
    print "data is %r" % data
    print "links is %r" % links

if __name__ == '__main__':
    url = "https://www.cs.wisc.edu/calendar/month/2018-09"
    if len(sys.argv) > 1:
        url = sys.argv[1]
    test_handler(url)
