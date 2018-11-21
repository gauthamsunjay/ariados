import importlib
import inspect
import os
import sys

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

            if not hasattr(f, "patterns"):
                return False

            return True

        self.members = inspect.getmembers(module, predicate=is_handler)
        import pdb; pdb.set_trace()

class HandlerManager(object):
    def __init__(self):
        # TODO find a way to deal with handlers not in handler dir
        # this assumes handler dir is a folder in the current dir. No slashes
        self.source_to_module = {}
        self._import_handlers()

        self.domain_to_regexs = {}
        self.domain_to_canon_fn = {}

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
            self.source_to_module[source.name] = source

    def get_handler_for_url(self, url):
        pass

    def canonicalize_url(self, url):
        pass

hm = HandlerManager()
