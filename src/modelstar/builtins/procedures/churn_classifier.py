import tqdm
import jinja2
import scipy
import joblib
import sklearn
import lightgbm
import numba
import requests
import psutil
import markupsafe
import importlib_metadata
import plotly
import packaging
import threadpoolctl
import matplotlib
import cycler
import category_encoders
import IPython

import pandas as pd
import numpy as np
from pandas import DataFrame
from modelstar import modelstar_record

# the main training function
# to do: auto-select sort metrics; output dashboard (metrics, feature importance, etc.)

import pycaret.classification as pcc


def train_churn_classifier(df: DataFrame,
                           target_col_name: str,
                           ignore_col_list: list) -> str:

    current_experiment = pcc.setup(df,
                                   target=target_col_name,
                                   ignore_features=ignore_col_list,
                                   html=False,  # https://github.com/pycaret/pycaret/issues/728
                                   system_log=False,  # Or, pass a StringIO for in memory logging
                                   verbose=False,
                                   profile=False
                                   )

    best_model = current_experiment.compare_models(sort='F1')

    compare_experiment = current_experiment.display_container[1]
    compare_experiment_html = current_experiment.display_container[1].to_html()

    modelstar_record(record_type='md', content='13 models have been trained. The selected model is %s with the highest F1 score of %f.' % (
        compare_experiment['Model'].iloc[0], compare_experiment['F1'].iloc[0]))
    modelstar_record(record_type='html', content=compare_experiment_html)

    # output a bunch of image files in the working directory
    # 'Confusion Matrix.png'
    cm_fig_html = current_experiment.plot_model(
        best_model, plot='confusion_matrix', save=True, display_format='html')
    modelstar_record(record_type='md', content='## Confusion Matrix')
    modelstar_record(record_type='html', content=cm_fig_html)

    # # 'AUC.png'
    auc_fig_html = current_experiment.plot_model(
        best_model, plot='auc', save=True, display_format='html')
    modelstar_record(record_type='md', content='## AOC Curve')
    modelstar_record(record_type='html', content=auc_fig_html)

    # 'Feature Importance.png'
    # to do: not every algorithm rated feature importance, need to handle it better
    try:
        feat_fig_html = current_experiment.plot_model(
            best_model, plot='feature', save=True, display_format='html')
        modelstar_record(
            record_type='md', content='## Which features are important to avoid churn?')
        modelstar_record(record_type='html', content=feat_fig_html)
    except:
        pass

    # modified to return the model, that can be manually saved.
    model_ = current_experiment.save_model(best_model, 'best_model')

    return 'success'