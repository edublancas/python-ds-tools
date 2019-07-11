"""
Task implementations
A Task is a unit of work that produces a persistent change (Product)
such as a bash or a SQL script
"""
import shlex
import subprocess
from subprocess import CalledProcessError
from pathlib import Path
import logging
from dstools.pipeline.tasks.Task import Task


class BashCommand(Task):
    """A task that runs bash command
    """

    def __init__(self, code, product, dag, name=None, params=None,
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

    def __init__(self, code, product, dag, name=None, params=None):
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

    def __init__(self, code, product, dag, name=None, params=None):
        super().__init__(code, product, dag, name, params)

    def run(self):
        # call it with product, upstream and the rest of the parameters
        self.code(**self.params)
