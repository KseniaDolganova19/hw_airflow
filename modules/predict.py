# <YOUR_IMPORTS>
import json
import logging
import os
from datetime import datetime

import dill
import pandas as pd
from sklearn.pipeline import Pipeline

#path = os.environ.get('PROJECT_PATH', '.')
path = os.path.expanduser('~/airflow_hw')

def get_latest_model() -> Pipeline:
    # TODO:
    # 1) Получаем путь к последней модели в папке
    search_dir = f"{path}/data/models"
    os.chdir(search_dir)
    files = filter(os.path.isfile, os.listdir(search_dir))
    files = [os.path.join(search_dir, f) for f in files]
    files.sort(key=lambda x: os.path.getmtime(x))
    path_latest_model = max(files, key=os.path.getctime)

    # 2) Читаем модель по полученному пути и возвращаем из функции
    with open(path_latest_model, 'rb') as file:
        model = dill.load(file)
    return model


def get_predicts() -> pd.DataFrame:
    # TODO:
    # 1) Заводим словарь preds, куда будем складывать предсказания
    preds = {}
    # 2) Получаем модель функцией get_latest_model
    model = get_latest_model()
    # 3) В цикле по директории с json-ами читаем каждый json,
    test_dir = f"{path}/data/test"
    os.chdir(test_dir)
    files = filter(os.path.isfile, os.listdir(test_dir))
    files = [os.path.join(test_dir, f) for f in files]
    for file in files:
        with open(file, 'r') as json_file:
            car = json.load(json_file)
    #    приводим его к датафрейму (из одной строки,
    #    напр. так df = pd.DataFrame(car, index=[0]))
        df = pd.DataFrame(car, index=[0])
    # 4) Делаем предсказание и сохраняем его в словарь preds
        pred = model.predict(df)
        preds[df.id[0]] = pred

    return pd.DataFrame(preds)


def predict() -> None:
    predictions = get_predicts()
    preds_filename = f'{path}/data/predictions/preds_{datetime.now().strftime("%Y%m%d%H%M")}.csv'
    predictions.to_csv(preds_filename, index=False)
    logging.info(f'Predictions are saved as {preds_filename}')


if __name__ == '__main__':
    predict()
