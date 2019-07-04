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


class Identifier:
    """Identifier abstract class
    """

    def __init__(self, s):
        self.needs_render = isinstance(s, Template)
        self.rendered = False
        self.s = s

        self.after_init_hook()

    def after_init_hook(self):
        raise NotImplementedError('Identifier subclasses must implement this '
                                  'method')

    def after_render_hook(self):
        raise NotImplementedError('Identifier subclasses must implement this '
                                  'method')

    def __str__(self):
        return self()

    def __repr__(self):
        return f'{type(self)}({self.s})'

    def render(self, params):
        if self.needs_render:
            if not self.rendered:
                self.s = self.s.render(params)
                self.rendered = True
                self.after_render_hook()
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
                                   'rendering the task first, call '
                                   'task.render() before reading '
                                   'the identifier or initialize with a str '
                                   'object, if this is part of a DAG, '
                                   'the task should render automatically')
            return self.s
        else:
            return self.s


class StringIdentifier(Identifier):
    """Identifier that represents a str object
    """

    def after_init_hook(self):
        # warn if object is not string, Path is ok too...
        if not self.needs_render and not (isinstance(self.s, str)
                                          or isinstance(self.s, Path)):
            # if no Template passed but parameter is not str, cast...
            warnings.warn('Initialized StringIdentifier with non-string '
                          f'object "{self.s}" type: '
                          f'{type(self.s)}, casting to str...')
            self.s = str(self.s)

    def after_render_hook(self):
        pass


class CodeIdentifier(Identifier):
    """
    A CodeIdentifier represents a piece of code in various forms:
    a Python callable, a language-agnostic string, a path to a soure code file
    or a jinja2.Template
    """

    def after_init_hook(self):
        valid_type = (callable(self.s)
                      or isinstance(self.s, str)
                      or isinstance(self.s, Path)
                      or isinstance(self.s, Template))
        if not valid_type:
            TypeError('Code must be a callable, str, pathlib.Path or '
                      f'jinja2.Template, got {type(self.s)}')

    @property
    def source(self):
        if callable(self.s):
            # TODO: i think this doesn't work sometime and dill has a function
            # that covers more use cases, check
            return inspect.getsource(self())
            return self()
        elif isinstance(self.s, Path):
            return self().read_text()
        elif isinstance(self.s, Template) or isinstance(self.s, str):
            return self()
        else:
            TypeError('Code must be a callable, str, pathlib.Path or '
                      f'jinja2.Template, got {type(self.code)}')

    def after_render_hook(self):
        pass
