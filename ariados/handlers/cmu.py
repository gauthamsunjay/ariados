from ariados.decorators import handler

SOURCE = 'cmu'
DOMAINS = [ 'cmu.edu' ]

@handler(r'/foo')
def myfoo_fn():
    pass
