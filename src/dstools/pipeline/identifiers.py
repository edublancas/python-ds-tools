"""
Identifiers are used by products to represent their persistent
representations, for example product, for example, File uses it to represent
the path to a file. Identifiers are lazy-loaded, they can be initialized
with a jinja2.Template and rendered before task execution, which makes
passing metadata between products and its upstream tasks possible.
"""
from pathlib import Path
import inspect
import warnings

from jinja2 import Template


class StringIdentifier:
    """An identifier that represents a string
    """

    def __init__(self, s):
        self.needs_render = isinstance(s, Template)
        self.rendered = False

        if not self.needs_render and not isinstance(s, str):
            # if no Template passed but parameter is not str, cast...
            warnings.warn('Initialized StringIdentifier with non-string '
                          f'object "{s}" type: {type(s)}, casting to str...')
            s = str(s)

        self._s = s

    def __str__(self):
        return self()

    def __repr__(self):
        return f'{type(self)}({self._s})'

    def render(self, params):
        if self.needs_render:
            if not self.rendered:
                self._s = self._s.render(params)
                self.rendered = True
            else:
                warnings.warn(f'Trying to render {repr(self)}, with was'
                              ' already rendered, skipping render...')

        return self

    def __call__(self):
        """Identifiers must be called from here
        """
        if self.needs_render:
            if not self.rendered:
                raise RuntimeError('Attempted to read Identifier '
                                   f'{repr(self)} '
                                   '(which was initialized with '
                                   'a jinja2.Template object) wihout '
                                   'rendering the DAG first, call '
                                   'dag.render() on the dag before reading '
                                   'the identifier or initialize with a str '
                                   'object')
            return self._s
        else:
            return self._s


class CodeIdentifier(StringIdentifier):
    """
    A CodeIdentifier represents a piece of code in various forms:
    a Python callable, a language-agnostic string, a path to a soure code file
    or a jinja2.Template
    """
    def __init__(self, code):
        self.needs_render = False
        self.rendered = False
        self._s = code

        if (callable(code)
            or isinstance(code, str)
                or isinstance(code, Path)):
            pass
        elif isinstance(code, Template):
            self.needs_render = True
        else:
            TypeError('Code must be a callable, str, pathlib.Path or '
                      f'jinja2.Template, got {type(code)}')

    @property
    def source(self):
        if callable(self._s):
            # TODO: i think this doesn't work sometime and dill has a function
            # that covers more use cases, check
            return inspect.getsource(self())
            return self()
        elif isinstance(self._s, Path):
            return self().read_text()
        elif isinstance(self._s, Template) or isinstance(self._s, str):
            return self()
        else:
            TypeError('Code must be a callable, str, pathlib.Path or '
                      f'jinja2.Template, got {type(self.code)}')
