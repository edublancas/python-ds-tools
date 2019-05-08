from jinja2 import Template
import warnings


class Identifier:

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
            self._s = self._s.render(params)

        self.rendered = True

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
