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
from modelstar import modelstar_record, modelstar_write_path, modelstar_register_pycaret_inference_udf

import pycaret.regression as pcr

def train_regressor(df: DataFrame,
                    target_col_name: str,
                    ignore_col_list: list) -> dict:

    current_experiment = pcr.setup(df,
                                   target=target_col_name,
                                   ignore_features=ignore_col_list,
                                   html=False,  # https://github.com/pycaret/pycaret/issues/728
                                   system_log=False,  # Or, pass a StringIO for in memory logging
                                   verbose=False,
                                   profile=False
                                   )
    
    modelstar_record(record_type='md',
                     content=f'**ML Type:     ** Regression')

    best_model = current_experiment.compare_models()

    compare_experiment = current_experiment.display_container[1]
    compare_experiment_html = current_experiment.display_container[1].to_html()

    # modelstar_record(record_type='md', content='18 models have been trained. The selected model is %s with the highest F1 score of %f.' % (
    #     compare_experiment['Model'].iloc[0], compare_experiment['F1'].iloc[0]))
    # modelstar_record(record_type='md', content='## ')
    modelstar_record(record_type='html', content=compare_experiment_html)

    # 'Feature Importance.png'
    # to do: not every algorithm rated feature importance, need to handle it better
    try:
        feat_fig_html = current_experiment.plot_model(
            best_model, plot='feature', save=True, display_format='html')
        modelstar_record(
            record_type='md', content='## Which features are important for performing **regression** on the dataset?')
        modelstar_record(record_type='html', content=feat_fig_html)
    except:
        pass

    # modified to return the model, that can be manually saved.
    model_ = current_experiment.save_model(best_model, 'best_model')
    modelstar_write_path(
        local_path='regression_model.joblib', write_object=model_)

    # Register the inference function
    handler_args_list = []
    ignore_df_cols = ignore_col_list
    ignore_df_cols.append(target_col_name)
    for col, dtype in df.dtypes.to_dict().items():
        if col not in ignore_df_cols:
            if dtype.name == 'object':
                arg_type = 'STRING'
            if dtype.name == 'bool':
                arg_type = 'BOOL'
            if dtype.name.startswith('int'):
                arg_type = 'NUMBER'
            if dtype.name.startswith('float'):
                arg_type = 'FLOAT'
            handler_args_list.append({'col_name': col, 'col_type': arg_type})

    modelstar_register_pycaret_inference_udf(
        function_name='predict_regressor', model_filename='regression_model.joblib.gz', handler_args=handler_args_list)

    return {'status': 'Model training success', 'inference_function': 'predict_regressor'}
