# HandlerManager figures out the right handler for a url

"""
hm = HandlerManager()

urls = [
    "https://www.cs.wisc.edu/calendar/month/2018-09/",
    "https://www.cs.wisc.edu/events/3984/",
]

for u in urls:
    canon_u = hm.canonicalize_url(u)
    print "canon for %s is %s" % (u, canon_u)
    handler = hm.get_handler_for_url(canon_u)
    print "handler for %s is %s" % (canon_u, handler)

    handler = hm.get_handler_for_url(u)
    print "handler for %s is %s" % (u, handler)
"""

import importlib
import inspect
import os
import sys
import urlparse

from ariados.common import Handler

class Source(object):
    def __init__(self, source, module_name, module):
        self.name = source
        self.module_name = module_name
        self.module = module

        if getattr(self.module, "SOURCE") != self.name:
            raise ValueError("Expected module to have SOURCE = %r" % self.name)

        def is_handler(f):
            if not inspect.isfunction(f):
                return False

            if not f.__module__ == self.module_name:
               return False

            if not hasattr(f, "handler"):
                return False

            if not isinstance(f.handler, Handler):
                return False

            return True

        self.handlers = [ i[1].handler for i in inspect.getmembers(module, predicate=is_handler) ]
        for h in self.handlers:
            h.name = '%s.%s' % (self.module_name, h.fn.__name__)

class HandlerManager(object):
    def __init__(self):
        # TODO find a way to deal with handlers not in handler dir
        # this assumes handler dir is a folder in the current dir. No slashes
        self.sources = []
        self.domain_to_handlers = {}
        self.domain_to_canonicalizer = {}

        # TODO allow download from s3 here (or maybe the caller should do it)
        self._import_handlers()

    def _import_handlers(self):
        files = os.listdir(os.path.join(os.path.dirname(__file__), 'handlers'))
        for f in files:
            if not f.endswith('.py'):
                continue

            if f.startswith('_') or f.startswith('.'):
                continue

            module_name = 'ariados.handlers.%s' % f[:-3]
            module = importlib.import_module(module_name)
            source = Source(f[:-3], module_name, module)
            self.sources.append(source)

        for source in self.sources:
            allowed_domains = source.module.DOMAINS
            # TODO preserve some ordering (like line number in file)
            # so that those are tried first. That way, more frequent urls get
            # regex matched earlier than the rest.
            for domain in allowed_domains:
                self.domain_to_handlers.setdefault(domain, []).extend(source.handlers)
                canon_fn = getattr(source.module, "canonicalize_url", None)
                if canon_fn is not None:
                    self.domain_to_canonicalizer[domain] = canon_fn

    def get_handler_for_url(self, url):
        parsed_url = urlparse.urlparse(url)
        # TODO allowed scheme?
        domain = parsed_url.netloc

        handlers_for_domain = self.domain_to_handlers.get(domain)
        if handlers_for_domain is None:
            return None

        # TODO allow query string matches in the future
        matches = filter(None, map(lambda h: h.match(parsed_url.path), handlers_for_domain))

        if len(matches) == 0:
            return None

        if len(matches) > 1:
            # What to do about more than one match?
            # TODO log as error?
            pass

        return matches[0][0]

    def canonicalize_url(self, url):
        parsed_url = urlparse.urlparse(url)

        domain = parsed_url.netloc
        canon_fn = self.domain_to_canonicalizer.get(domain)
        if canon_fn is None:
            return url

        return canon_fn(url)
