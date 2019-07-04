"""
Environment management
"""
import logging
from warnings import warn
from itertools import chain
from pathlib import Path
from glob import iglob
from io import StringIO
import getpass

from dstools.FrozenJSON import FrozenJSON
from dstools.path import PathManager
from dstools import repo

import yaml
from jinja2 import Template


class Env:
    """
    Env provides a clean and consistent way of managing environment and
    configuration settings. Its simplest usage provides access to settings
    specified via an `env.yaml`.

    Settings managed by Env are intended to be runtime constant (they are NOT
    intended to be used as global variables). For example you might want
    to store database URIs. Storing sensitive information is discouraged as
    yaml files are plain text. Use `keyring` for that instead.

    All sections are optional, but if there is a path section, all values
    inside that section will be casted to pathlib.Path objects, expanduser()
    is applied so "~" can be used.

    There are two wildcards available "{{user}}" (returns the current user)
    and "{{git_branch}}" (returns the current git branch by looking in the
    env.yaml file location)

    Examples
    --------

    Basic usage

    >>> from dstools import Env
    >>> env = Env()
    >>> env.db.uri # traverse the yaml tree structure using dot notation
    >>> env.path.raw # returns an absolute path to the raw data
    """
    __path_to_env = None

    def __init__(self, path_to_env=None):
        self.logger = logging.getLogger(__name__)

        # if not env has been set...
        if Env.__path_to_env is None:

            # try to set it if no argument was provided
            if path_to_env is None:

                path_to_env = find_env()

                if path_to_env is None:
                    raise FileNotFoundError("Couldn't find env.yaml")

            # resolve it
            path_to_env = Path(path_to_env).resolve()
            Env.__path_to_env = path_to_env

        # if an environment has been set...
        else:
            # if no argument provided, just use whatever env was already used
            if path_to_env is None:
                path_to_env = Env.__path_to_env
                self.logger.info('Already instantiated Env, loading it from '
                                 f'{path_to_env}...')

            # otherwise, resolve argument and warng the user if there is
            # conflcit
            else:
                path_to_env = Path(path_to_env).resolve()

                if Env.__path_to_env != path_to_env:
                    warn('Env was already instantiated using file '
                         f'{Env.__path_to_env} but new '
                         'instance was created using '
                         f'{path_to_env}, it is not recommended to '
                         'have more than one environment per project')

        self._path_to_env = path_to_env

        self._env_content = self.load(path_to_env)

        self._name = _get_name(path_to_env)
        self._path = PathManager(path_to_env, self)

    def __repr__(self):
        return f'Env loaded from {self._path_to_env}'

    def __dir__(self):
        return dir(self._env_content)

    @property
    def name(self):
        return self._name

    @property
    def path(self):
        """
        path manager, returnd paths to directories specified in your env
        """
        return self._path

    def __getattr__(self, key):
        return getattr(self._env_content, key)

    def __getitem__(self, key):
        return self._env_content[key]

    def get_metadata(self):
        """Get env metadata such as git hash, last commit timestamp
        """
        return repo.get_env_metadata(self.path.home)

    def load(self, path_to_env):
        path_to_env = Path(path_to_env)
        home = path_to_env.parent
        env_content = path_to_env.read_text()

        params = dict(user=getpass.getuser())

        # only try to find git branch if {{git_branch is used}}
        if '{{git_branch}}' in env_content:
            params['git_branch'] = repo.get_env_metadata(home)['git_branch']

        s = Template(env_content).render(**params)

        with StringIO(s) as f:
            content = yaml.load(f)

        env = FrozenJSON(content)

        return env


def find_env(max_levels_up=6):
    def levels_up(n):
        return chain.from_iterable(iglob('../' * i + '**')
                                   for i in range(n + 1))

    path_to_env = None

    for filename in levels_up(max_levels_up):
        p = Path(filename)

        if p.name == 'env.yaml':
            path_to_env = filename
            break

    return path_to_env


def load_config(config_file):
    """Loads configuration file, asumes file is in config/

    Parameters
    ----------
    file: str
        As returned from __file__
    """
    project_dir = Env().project_dir
    return Path(project_dir, 'config', config_file).absolute()


def _get_name(path_to_env):
    filename = str(Path(path_to_env).name)

    err = ValueError('Wrong filename, must be either env.{name}.yaml '
                     'or env.yaml')

    elements = filename.split('.')

    if len(elements) == 2:
        # no name case
        env, _ = elements
        name = 'root'
    elif len(elements) == 3:
        # name
        env, name, _ = elements
    else:
        raise err

    if env != 'env':
        raise err

    return name
