from pathlib import Path
import pytest
from dstools.env import _get_name, Env
from dstools.reproducibility import make_logger_file


def test_assigns_default_name():
    assert _get_name('path/to/env.yaml') == 'default'


def test_can_extract_name():
    assert _get_name('path/to/env.my_name.yaml') == 'my_name'


def test_raises_error_if_wrong_format():
    with pytest.raises(ValueError):
        _get_name('path/to/wrong.my_name.yaml')


def test_can_instantiate_env_if_located_in_current_dir(move_to_sample):
    Env()
    Env._destroy()


def test_can_instantiate_env_if_located_in_child_dir(move_to_module):
    Env()
    Env._destroy()


def test_make_logger_file(path_to_env, path_to_source_code):
    path_to_home = Env(path_to_env).path.home
    path_to_log = make_logger_file(path_to_source_code)
    path_to_log_dir = path_to_log.relative_to(path_to_home).parent

    assert path_to_log_dir == Path('log', 'src', 'pkg', 'module', 'functions')
