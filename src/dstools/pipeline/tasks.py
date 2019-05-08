"""
A Task is a unit of work that produces a persistent change (Product)
such as a bash or a SQL script
"""
import shlex
import subprocess
from subprocess import CalledProcessError
from pathlib import Path
import logging
from datetime import datetime
from dstools.pipeline import util
from dstools.pipeline.products import Product, MetaProduct
from dstools.pipeline.identifiers import CodeIdentifier
from dstools.util import isiterable


class TaskGroup:
    """
    A collection of Tasks, used internally for enabling operador overloading

    (task1 + task2) >> task3
    """

    def __init__(self, tasks):
        self.tasks = tasks

    def __iter__(self):
        for t in self.tasks:
            yield t

    def __add__(self, other):
        if isiterable(other):
            return TaskGroup(list(self.tasks) + list(other))
        else:
            return TaskGroup(list(self.tasks) + [other])

    def __radd__(self, other):
        if isiterable(other):
            return TaskGroup(list(other) + list(self.tasks))
        else:
            return TaskGroup([other] + list(self.tasks))

    def set_upstream(self, other):
        if isiterable(other):
            for t in self.tasks:
                for o in other:
                    t.set_upstream(other)
        else:
            for t in self.tasks:
                t.set_upstream(other)

    # FIXME: implement render

    def __rshift__(self, other):
        other.set_upstream(self)
        # return other so a >> b >> c works
        return other


class Task:
    """A task represents a unit of work

    Parameters
    ----------
    code: callable, Path, str
    """

    def __init__(self, code, product, dag, name, params={}):
        self._upstream = []
        self._upstream_by_name = {}
        self._name = name

        self.params = params

        self._code = CodeIdentifier(code)

        if isinstance(product, Product):
            self._product = product
        else:
            self._product = MetaProduct(product)

        self._logger = logging.getLogger(__name__)

        self.product.task = self

        dag.add_task(self)

    @property
    def name(self):
        return self._name

    @property
    def source_code(self):
        return self._code.source

    @property
    def product(self):
        return self._product

    @property
    def code(self):
        return self._code()

    @property
    def upstream(self):
        return self._upstream

    @property
    def upstream_by_name(self):
        return self._upstream_by_name

    def run(self):
        raise NotImplementedError('You have to implement this method')

    def set_upstream(self, other):
        if isiterable(other):
            for o in other:
                self._upstream.append(o)
                self._upstream_by_name[o.name] = o
        else:
            self._upstream.append(other)
            self._upstream_by_name[other.name] = other

    def __rshift__(self, other):
        """ a >> b is the same as b.set_upstream(a)
        """
        other.set_upstream(self)
        # return other so a >> b >> c works
        return other

    def __add__(self, other):
        """ a + b means TaskGroup([a, b])
        """
        if isiterable(other):
            return TaskGroup([self] + list(other))
        else:
            return TaskGroup((self, other))

    def build(self, force=False):

        # NOTE: should i fetch metadata here? I need to make sure I have
        # the latest before building

        self._logger.info(f'-----\nChecking {repr(self)}....')

        run = False

        # check dependencies only if the product exists and there is metadata
        if self.product.exists() and self.product.metadata is not None:
            outdated_data_deps = self.product.outdated_data_dependencies()
            outdated_code_dep = self.product.outdated_code_dependency()

            if outdated_data_deps:
                run = True
                self._logger.info('Outdated data deps...')
            else:
                self._logger.info('Up-to-date data deps...')

            if outdated_code_dep:
                run = True
                self._logger.info('Outdated code dep...')
            else:
                self._logger.info('Up-to-date code dep...')
        else:
            self._logger.info('Product does not exist...')
            run = True

        if run or force:
            if force:
                self._logger.info('Forcing...')

            self._logger.info(f'Running {repr(self)}')

            self.run()

            # TODO: should check if job ran successfully, if not,
            # stop execution

            # update metadata
            self.product.pre_save_metadata_hook()
            self.product.timestamp = datetime.now().timestamp()
            self.product.stored_source_code = self.source_code
            self.product.save_metadata()

            # TODO: also check that the Products were updated:
            # if they did not exist, they must exist now, if they alredy
            # exist, timestamp must be recent equal to the datetime.now()
            # used. maybe run fetch metadata again and validate?

        else:
            self._logger.info(f'No need to run {repr(self)}')

        self._logger.info('-----\n')

    def status(self):
        """Prints the current task status
        """
        p = self.product

        outd_code = p.outdated_code_dependency()

        out = ''

        if p.timestamp is not None:
            dt = datetime.fromtimestamp(p.timestamp).strftime('%b %m, %y at %H:%M')
            out += f'* Last updated: {dt}\n'
        else:
            out += f'* Timestamp is None\n'

        out += f'* Oudated data dependencies: {p.outdated_data_dependencies()}'
        out += f'\n* Oudated code dependency: {outd_code}'

        if outd_code:
            out += '\n\nCODE DIFF\n*********\n'
            out += util.diff_strings(p.stored_source_code, self.source_code)
            out += '\n*********'

        print(out)
        return out

    def render(self):
        """
        Renders code and product, all upstream tasks must have been rendered
        first
        """
        up = {n: t.product.identifier for n, t
              in self.upstream_by_name.items()}

        # pass identifier objects only
        params = {**self.params, **up}

        # FIXME: need to render code here as well
        try:
            self.product.render(params)
        except Exception as e:
            raise RuntimeError(f'Error rendering product {self.product} from '
                               f'task {self} with params '
                               f'{params}. Exception: {e}')

        params['me'] = self.product.identifier

        self._code.render(params)

    def __repr__(self):
        return f'{type(self).__name__}: {self.name} -> {repr(self.product)}'

    def short_repr(self):
        def short(s):
            max_l = 30
            return s if len(s) <= max_l else s[:max_l-3]+'...'

        return f'{short(self.name)} -> \n{short(self.product.short_repr())}'


class BashCommand(Task):
    """A task that runs bash command
    """

    def __init__(self, code, product, dag, name, params={},
                 subprocess_run_kwargs={'stderr': subprocess.PIPE,
                                        'stdout': subprocess.PIPE,
                                        'shell': False},
                 split_source_code=True):
        super().__init__(code, product, dag, name, params)
        self.split_source_code = split_source_code
        self.subprocess_run_kwargs = subprocess_run_kwargs
        self._logger = logging.getLogger(__name__)

    def run(self):
        source_code = (shlex.split(self.source_code) if self.split_source_code
                       else self.source_code)

        res = subprocess.run(source_code,
                             **self.subprocess_run_kwargs)

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{self.source_code} returned stdout: '
                              f'{res.stdout} and stderr: {res.stderr} '
                              f'and exit status {res.returncode}')
            raise CalledProcessError(res.returncode, self.source_code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {res.stdout},'
                              f' stderr: {res.stderr}')


class ScriptTask(Task):
    """A task that runs a generic script
    """
    _INTERPRETER = None

    def __init__(self, code, product, dag, name, params={}):
        if not isinstance(code, Path):
            raise ValueError(f'{type(self).__name__} must be called with '
                             'a pathlib.Path object in the code '
                             'parameter')

        super().__init__(code, product, dag, name, params)

    def run(self):
        if self._INTERPRETER is None:
            raise ValueError(f'{type(self).__name__}: subclasses must '
                             'declare an interpreter')
        res = subprocess.run([self._INTERPRETER,
                              self.code],
                             stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE)

        if res.returncode != 0:
            # log source code without expanded params
            self._logger.info(f'{self.source_code} returned stdout: '
                              f'{res.stdout} and stderr: {res.stderr} '
                              f'and exit status {res.returncode}')
            raise CalledProcessError(res.returncode, self.source_code)
        else:
            self._logger.info(f'Finished running {self}. stdout: {res.stdout},'
                              f' stderr: {res.stderr}')


class BashScript(ScriptTask):
    """A task that runs a bash script
    """
    _INTERPRETER = 'bash'


class PythonScript(ScriptTask):
    """A task that runs a python script
    """
    _INTERPRETER = 'python'


class PythonCallable(Task):
    """A task that runs a Python callabel (i.e.  a function)
    """
    def __init__(self, code, product, dag, name, params={}):
        super().__init__(code, product, dag, name, params)

    def compile_source_code(self):
        pass

    def run(self):
        self.code(**self.params)
