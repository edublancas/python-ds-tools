"""
%load_ext autoreload
%autoreload 2
"""
import logging

from dstools.pipeline.products import File, PostgresRelation
from dstools.pipeline.tasks import (BashCommand, PythonCallable,
                                    SQLScript)
from dstools.pipeline.dag import DAG
from dstools.pipeline.clients import SQLAlchemyClient
from dstools import testing
from dstools import Env
from dstools import mkfilename

from train import train_and_save_report
import util
from download_dataset import download_dataset
from sample import sample


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


env = Env()
home = env.path.home
path_to_sample = env.path.input / 'sample'

(env.path.input / 'raw').mkdir(exist_ok=True, parents=True)
path_to_sample.mkdir(exist_ok=True)

uri = util.load_db_uri()
pg_client = SQLAlchemyClient(uri)

dag = DAG()
dag.clients[PostgresRelation] = pg_client
dag.clients[SQLScript] = pg_client

dag.product.delete()


get_data = BashCommand((home / 'get_data.sh').read_text(),
                       (File(env.path.input / 'raw' / 'red.csv'),
                        File(env.path.input / 'raw' / 'white.csv'),
                        File(env.path.input / 'raw' / 'names')),
                       dag,
                       name='get_data',
                       split_source_code=False)

sample = PythonCallable(sample,
                        (File(env.path.input / 'sample' / 'red.csv'),
                         File(env.path.input / 'sample' / 'white.csv')),
                        name='sample',
                        dag=dag)
get_data >> sample

red_task = BashCommand(('csvsql --db {{uri}} --tables {{product.name}} --insert {{upstream["sample"][0]}} '
                        '--overwrite'),
                       PostgresRelation(('public', 'red', 'table')),
                       dag,
                       params=dict(uri=uri),
                       split_source_code=False)
sample >> red_task

white_task = BashCommand(('csvsql --db {{uri}} --tables {{product.name}} --insert {{upstream["sample"][1]}} '
                          '--overwrite'),
                         PostgresRelation(('public', 'white', 'table')),
                         dag,
                         params=dict(uri=uri),
                         split_source_code=False)
sample >> white_task


wine_task = SQLScript(home / 'sql' / 'create_wine.sql',
                      PostgresRelation(('public', 'wine', 'table')),
                      dag)
(red_task + white_task) >> wine_task


dataset_task = SQLScript(home / 'sql' / 'create_dataset.sql',
                         PostgresRelation(('public', 'dataset', 'table')),
                         dag)
wine_task >> dataset_task


training_task = SQLScript(home / 'sql' / 'create_training.sql',
                          PostgresRelation(('public', 'training', 'table')),
                          dag)
dataset_task >> training_task


testing_table = PostgresRelation(('public', 'testing', 'table'))
testing_table.tests = [testing.Postgres.no_nas_in_column('label')]
testing_task = SQLScript(home / 'sql' / 'create_testing.sql',
                         testing_table, dag)

dataset_task >> testing_task


path_to_dataset = env.path.input / 'datasets'
params = dict(path_to_dataset=path_to_dataset)
download_task = PythonCallable(download_dataset,
                               (File(path_to_dataset / 'training.csv'),
                                File(path_to_dataset / 'testing.csv')),
                               dag, params=params)
training_task >> download_task
testing_task >> download_task


path_to_report = env.path.input / 'reports' / mkfilename('report.txt')
params = dict(path_to_dataset=path_to_dataset,
              path_to_report=path_to_report)
train_task = PythonCallable(train_and_save_report, File(
    path_to_report), dag, params=params)
download_task >> train_task

# dag.plot()

stats = dag.build()

# print(str(stats))
