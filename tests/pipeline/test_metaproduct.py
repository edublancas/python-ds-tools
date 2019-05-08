from pathlib import Path

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand
from dstools.pipeline.products import File


def test_can_create_task_with_more_than_one_product(tmp_directory):
    dag = DAG()

    fa = Path('a.txt')
    fb = Path('b.txt')
    fc = Path('c.txt')

    ta = BashCommand('touch a.txt b.txt', (File(fa), File(fb)), dag, 'ta')
    tc = BashCommand('touch c.txt', File(fc), dag, 'tc')

    ta >> tc

    dag.build()
