from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import Task, BashCommand
from dstools.pipeline.products import File


def test_non_existent_file():
    dag = DAG()
    f = File('file.txt')
    ta = Task('echo hi', f, dag, 'ta')

    assert not f.exists()
    assert f.outdated()
    assert f.outdated_code_dependency()
    assert not f.outdated_data_dependencies()


def test_outdated_data_simple_dependency(tmp_directory):
    """ A -> B
    """
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')

    ta = BashCommand('touch a.txt', File(fa), dag, 'ta')
    tb = BashCommand('touch b.txt', File(fb), dag, 'tb')

    tb.set_upstream(ta)

    assert not ta.product.exists()
    assert not tb.product.exists()
    assert ta.product.outdated()
    assert tb.product.outdated()

    dag.build()

    # they both exist now
    assert ta.product.exists()
    assert tb.product.exists()

    # and arent outdated...
    assert not ta.product.outdated()
    assert not tb.product.outdated()

    # let's make b outdated
    ta.build(force=True)

    assert not ta.product.outdated()
    assert tb.product.outdated()


def test_many_upstream(tmp_directory):
    """ {A, B} -> C
    """
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch a.txt', File(fa), dag, 'ta')
    tb = BashCommand('touch b.txt', File(fb), dag, 'tb')
    tc = BashCommand('touch c.txt', File(fc), dag, 'tc')

    tc.set_upstream(ta)
    tc.set_upstream(tb)

    dag.build()

    assert ta.product.exists()
    assert tb.product.exists()
    assert tc.product.exists()

    assert not ta.product.outdated()
    assert not tb.product.outdated()
    assert not tc.product.outdated()

    ta.build(force=True)

    assert not ta.product.outdated()
    assert not tb.product.outdated()
    assert tc.product.outdated()

    dag.build()
    tb.build(force=True)

    assert not ta.product.outdated()
    assert not tb.product.outdated()
    assert tc.product.outdated()


def test_many_downstream():
    """ A -> {B, C}
    """
    pass


def test_chained_dependency():
    """ A -> B -> C
    """
    pass


def test_can_create_task_with_many_products():
    dag = DAG()
    fa1 = File('a1.txt')
    fa2 = File('a2.txt')
    ta = Task('echo hi', [fa1, fa2], dag, 'ta')

    assert not ta.product.exists()
    assert ta.product.outdated()
    assert ta.product.outdated_code_dependency()
    assert not ta.product.outdated_data_dependencies()


def test_overloaded_operators():
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch a.txt', File(fa), dag, 'ta')
    tb = BashCommand('touch b.txt', File(fb), dag, 'tb')
    tc = BashCommand('touch c.txt', File(fc), dag, 'tc')

    ta >> tb >> tc

    assert not ta.upstream
    assert tb in tc.upstream
    assert ta in tb.upstream


def test_adding_tasks():
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch a.txt', File(fa), dag, 'ta')
    tb = BashCommand('touch b.txt', File(fb), dag, 'tb')
    tc = BashCommand('touch c.txt', File(fc), dag, 'tc')

    assert list((ta + tb).tasks) == [ta, tb]
    assert list((tb + ta).tasks) == [tb, ta]
    assert list((ta + tb + tc).tasks) == [ta, tb, tc]
    assert list(((ta + tb) + tc).tasks) == [ta, tb, tc]
    assert list((ta + (tb + tc)).tasks) == [ta, tb, tc]


def test_adding_tasks_left():
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch a.txt', File(fa), dag, 'ta')
    tb = BashCommand('touch b.txt', File(fb), dag, 'tb')
    tc = BashCommand('touch c.txt', File(fc), dag, 'tc')

    (ta + tb) >> tc

    assert not ta.upstream
    assert not tb.upstream
    assert tc.upstream == [ta, tb]


def test_adding_tasks_right():
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch a.txt', File(fa), dag, 'ta')
    tb = BashCommand('touch b.txt', File(fb), dag, 'tb')
    tc = BashCommand('touch c.txt', File(fc), dag, 'tc')

    ta >> (tb + tc)

    assert not ta.upstream
    assert tb.upstream == [ta]
    assert tc.upstream == [ta]
