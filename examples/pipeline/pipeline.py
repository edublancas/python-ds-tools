"""
%load_ext autoreload
%autoreload 2
"""
import logging
from pathlib import Path
from jinja2 import Template

from dstools.pipeline.products import File
from dstools.pipeline.tasks import (BashCommand, BashScript, PythonCallable,
                                    PythonScript)
from dstools.pipeline import postgres as pg
from dstools.pipeline.dag import DAG
from dstools import testing
from dstools import Env
from dstools import mkfilename

from train import train_and_save_report
import util
from download_dataset import download_dataset


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


env = Env()
home = env.path.home
path_to_sample = env.path.input / 'sample'

path_to_sample.mkdir(exist_ok=True)

pg.CONN = util.open_db_conn()
db = util.load_db_credentials()

dag = DAG()

get_data = BashScript(home / 'get_data.sh',
                      File(env.path.input / 'raw' / 'red.csv'),
                      dag, 'get_data')

sample = PythonScript(home / 'sample.py',
                      File(env.path.input / 'sample' / 'red.csv'),
                      dag, 'sample')
get_data >> sample

red_path = path_to_sample / 'red.csv'
red_task = BashCommand(Template('csvsql --db {{db}} --tables red --insert {{path}} '
                                '--overwrite'),
                       pg.PostgresRelation(('public', 'red', 'table')),
                       dag, 'red',
                       params=dict(db=db['uri'], path=red_path))
sample >> red_task

white_path = Path(path_to_sample / 'white.csv')
white_task = BashCommand(Template('csvsql --db {{db}} --tables white --insert {{path}} '
                                  '--overwrite'),
                         pg.PostgresRelation(('public', 'white', 'table')),
                         dag, 'white',
                         params=dict(db=db['uri'], path=white_path))
sample >> white_task


wine_task = pg.PostgresScript(home / 'sql' / 'create_wine.sql',
                              pg.PostgresRelation(('public', 'wine', 'table')),
                              dag, 'wine')
(red_task + white_task) >> wine_task


dataset_task = pg.PostgresScript(home / 'sql' / 'create_dataset.sql',
                                 pg.PostgresRelation(
                                     ('public', 'dataset', 'table')),
                                 dag, 'dataset')
wine_task >> dataset_task


training_task = pg.PostgresScript(home / 'sql' / 'create_training.sql',
                                  pg.PostgresRelation(
                                      ('public', 'training', 'table')),
                                  dag, 'training')
dataset_task >> training_task


testing_table = pg.PostgresRelation(('public', 'testing', 'table'))
testing_table.tests = [testing.Postgres.no_nas_in_column('label')]
testing_task = pg.PostgresScript(home / 'sql' / 'create_testing.sql',
                                 testing_table, dag, 'testing')

dataset_task >> testing_task


path_to_dataset = env.path.input / 'datasets'
params = dict(path_to_dataset=path_to_dataset)
download_task = PythonCallable(download_dataset,
                               File(path_to_dataset / 'training.csv'),
                               dag, 'download', params=params)
training_task >> download_task
testing_task >> download_task


path_to_report = env.path.input / 'reports' / mkfilename('report.txt')
params = dict(path_to_dataset=path_to_dataset,
              path_to_report=path_to_report)
train_task = PythonCallable(train_and_save_report, File(
    path_to_report), dag, 'train', params=params)
download_task >> train_task

# dag.plot()

stats = dag.build()

# print(str(stats))

pg.CONN.close()
