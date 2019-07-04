from pathlib import Path


class PathManager:

    def __init__(self, path_to_env, env):
        self._home = Path(path_to_env).resolve().parent
        self._env = env

    @property
    def home(self):
        """Project's home folder
        """
        return self._home

    def __getattr__(self, key):
        path = Path(getattr(self._env._env_content.path, key))
        path.mkdir(parents=True, exist_ok=True)
        return path
