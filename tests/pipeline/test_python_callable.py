from pathlib import Path
from dstools.pipeline import DAG
from dstools.pipeline.tasks import PythonCallable
from dstools.pipeline.products import File


def fn(product, upstream, a):
    Path(product).write_text('things')


def test_params_are_accesible_after_init():
    dag = DAG()
    t = PythonCallable(fn, File('file.txt'), dag, 'callable',
                       params=dict(a=1))
    assert t.params == dict(a=1)


def test_upstream_and_me_are_added():
    dag = DAG()
    t = PythonCallable(fn, File('file.txt'), dag, 'callable',
                       params=dict(a=1))
    dag.render()
    assert t.params == dict(a=1, upstream=dict(), product='file.txt')


def test_can_execute_python_callable(tmp_directory):
    dag = DAG()
    PythonCallable(fn, File('file.txt'), dag, 'callable',
                   params=dict(a=1))
    assert dag.build()
