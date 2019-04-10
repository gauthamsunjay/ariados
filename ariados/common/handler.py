class Handler(object):
    def __init__(self, fn):
        self.fn = fn

        self.name = None
        # If any of these patterns match, it calls the handler.
        self.patterns = []

    def match(self, s, qparams=None):
        # TODO flags
        for p, q in self.patterns:
            match = p.match(s)
            if match is None:
                continue

            if q is None:
                return (self, match)

            if qparams is not None:
                defined_qparams = set(q)
                given_qparams = set([i[0] for i in qparams])
                if defined_qparams == given_qparams:
                    return (self, match)
            
        return None

    def validate(self):
        assert self.name is not None

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    def __hash__(self):
        return hash(self.fn)

    def __eq__(self, o):
        if not isinstance(o, Handler):
            return False
        return self.fn is o.fn

    def __ne__(self, o):
        return not self == o

    def __str__(self):
        if self.name is not None:
            return "Handler(%s)" % self.name

        return "Handler(fn=%s)" % self.fn.__name__

    __unicode__ = __str__
    __repr__ = __str__
