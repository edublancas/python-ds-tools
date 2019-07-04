import subprocess

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand
from dstools.pipeline.products import File

from jinja2 import Template
import pytest


@pytest.fixture
def dag():
    dag = DAG()

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    t1 = BashCommand('echo a > 1.txt ', File('1.txt'), dag,
                     't1', {}, kwargs, False)

    t2 = BashCommand(Template('cat {{upstream["t1"]}} > {{product}}'
                     '&& echo b >> {{product}} '),
                     File(Template('2_{{upstream["t1"]}}')),
                     dag,
                     't2', {}, kwargs, False)

    t3 = BashCommand(Template('cat {{upstream["t2"]}} > {{product}} '
                     '&& echo c >> {{product}}'),
                     File(Template('3_{{upstream["t2"]}}')), dag,
                     't3', {}, kwargs, False)

    t1 >> t2 >> t3

    return dag


def test_can_render_templates_in_products(dag, tmp_directory):

    t2 = dag['t2']
    t3 = dag['t3']

    dag.render()

    assert str(t3.product) == '3_2_1.txt'
    assert str(t2.product) == '2_1.txt'


def test_can_render_with_postgres_products(dag, tmp_directory):
    pass


def test_can_render_templates_in_code(dag, tmp_directory):
    pass


def test_can_build_dag_with_templates(dag, tmp_directory):
    pass


def test_rendering_dag_also_renders_upstream_outside_dag(tmp_directory):
    sub_dag = DAG('sub_dag')

    fa = Template('a.txt')
    ta = BashCommand(Template('touch a.txt'), File(fa), sub_dag, 'ta')
    tb = BashCommand('touch b.txt', File('b.txt'), sub_dag, 'tb')

    dag = DAG('dag')

    tc = BashCommand('touch c.txt', File('c.txt'), dag, 'tc')
    td = BashCommand('touch d.txt', File('d.txt'), dag, 'td')

    ta >> tb >> tc >> td

    dag.build()
