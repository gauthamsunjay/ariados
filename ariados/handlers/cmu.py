from ariados.decorators import pattern

SOURCE = 'cmu'
DOMAINS = [ 'cmu.edu' ]

@pattern(r'/foo')
def myfoo_fn():
    pass
