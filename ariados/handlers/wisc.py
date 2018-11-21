from ariados.decorators import pattern

SOURCE = 'wisc'
DOMAINS = [ 'www.wisc.edu', 'www.cs.wisc.edu' ]

@pattern(r'/bar')
def mybar_fn():
    pass
