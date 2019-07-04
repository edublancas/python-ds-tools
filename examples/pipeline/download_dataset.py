"""
Pre-process red.csv and white.csv
"""
import util
import pandas as pd


def download_dataset(product, upstream, path_to_dataset):
    path_to_dataset.mkdir(exist_ok=True)

    conn = util.open_db_conn()

    df_training = pd.read_sql('SELECT * FROM training', conn)
    df_testing = pd.read_sql('SELECT * FROM testing', conn)

    df_training.to_csv(path_to_dataset / 'training.csv', index=False)
    df_testing.to_csv(path_to_dataset / 'testing.csv', index=False)
