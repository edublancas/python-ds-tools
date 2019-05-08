import subprocess
from pathlib import Path

from jinja2 import Template

from dstools.pipeline.dag import DAG
from dstools.pipeline.tasks import BashCommand
from dstools.pipeline.products import File


def test_passing_t_and_up_in_bashcommand(tmp_directory):
    dag = DAG()

    kwargs = {'stderr': subprocess.PIPE,
              'stdout': subprocess.PIPE,
              'shell': True}

    t1 = BashCommand('echo a > {{product}} ', File('1.txt'), dag,
                     't1', {}, kwargs, False)

    t2 = BashCommand('cat {{t1.product}} > {{product}}'
                     '&& echo b >> {{product}} ',
                     File('{{t1.path_to_file.name}}_2.txt'),
                     dag,
                     't2', {}, kwargs, False)

    t3 = BashCommand('cat {{t2.product}} > {{product}} '
                     '&& echo c >> {{product}}',
                     File('{{t2.path_to_file.name}}_3.txt'), dag,
                     't3', {}, kwargs, False)

    t1 >> t2 >> t3

    dag.build()

    # assert t.product.read_text() == 'a\nb\nc\n'
