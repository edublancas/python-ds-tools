"""
Testing that upstream tasks metadata is available
"""
import subprocess
from pathlib import Path

from jinja2 import Template

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand
from dstools.pipeline.products import File
from dstools.pipeline import postgres as pg


def test_passing_upstream_and_product_in_bashcommand(tmp_directory):
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    ta = BashCommand(Template('echo a > {{product}} '), File(fa), dag,
                     'ta', {}, kwargs, False)
    tb = BashCommand(Template('cat {{upstream["ta"]}} > {{product}}'
                     '&& echo b >> {{product}} '), File(fb), dag,
                     'tb', {}, kwargs, False)
    tc = BashCommand(Template('cat {{upstream["tb"]}} > {{product}} '
                     '&& echo c >> {{product}}'), File(fc), dag,
                     'tc', {}, kwargs, False)

    ta >> tb >> tc

    dag.build()

    assert fc.read_text() == 'a\nb\nc\n'


def test_passing_upstream_and_product_in_postgres(open_conn):
    dag = DAG()

    cur = open_conn.cursor()
    cur.execute('drop table if exists public.series;')
    open_conn.commit()
    cur.close()

    ta_t = Template("""begin;
              drop table if exists {{product}};
              create table {{product}} as
              select * from generate_series(0, 15) as n;
              commit;""")
    ta_rel = pg.PostgresRelation(('public', 'series', 'table'))
    ta = pg.PostgresScript(ta_t, ta_rel, dag, 'ta')

    dag.build()

    assert ta_rel.exists()
