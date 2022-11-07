from modelstar import modelstar_write_path, modelstar_record
from prophet.plot import plot_plotly, plot_components_plotly
import pandas as pd
from pandas import DataFrame
from prophet import Prophet
import plotly

__version__ = 'V1'


def validate_forecast_data(df):
    # placeholder for more data validation for df[['ds', 'y']]
    pass


def prepare_data(df: DataFrame, ds_col_name: str, y_col_name: str) -> DataFrame:

    # select two cols from input dataframe
    try:
        df = df[[ds_col_name, y_col_name]]
    except KeyError as error:
        print(error)

    # rename the cols
    df = df.rename({ds_col_name: 'ds', y_col_name: 'y'}, axis=1)

    # confirm data type for each cols
    df['ds'] = pd.DatetimeIndex(df['ds'])

    # sort df
    df = df.sort_values('ds')

    # extra validations
    validate_forecast_data(df)

    return df


# main function to be deployed as sproc
def univariate_time_series_forecast(df: DataFrame, ds_col_name: str, y_col_name: str, periods: int, freqency: str) -> DataFrame:

    df = prepare_data(df, ds_col_name, y_col_name)

    model = Prophet()
    model.fit(df)

    future = model.make_future_dataframe(periods=periods, freq=freqency)
    forecast = model.predict(future)

    fig1 = plot_plotly(model, forecast, xlabel=ds_col_name, ylabel=y_col_name)

    fig1.update_layout(showlegend=True,
                       title_text='History (Actual) and Forecast (Predicted) Data Overlay',
                       title_x=0.5)

    # only show legend for 2 main traces
    for trace in fig1['data']:
        if (trace['name'] not in ['Actual', 'Predicted']):
            trace['showlegend'] = False

    fig2 = plot_components_plotly(model, forecast)
    fig2.update_layout(title_text='Forecast Trends',
                       title_x=0.5)

    modelstar_record(
        record_type='md', content='Plot of the forecast using the Prophet\'s plot method and passing in your forecast dataframe.')

    modelstar_record(record_type='html', content=fig1.to_html(
        full_html=False, include_plotlyjs='cdn'))

    modelstar_record(record_type='md', content='Plot of the forecast by components, by using Prophet\'s plot_components method.  By default you see the trend, yearly seasonality, and weekly seasonality of the time series.  If you include holidays, you will see those here, too.')

    modelstar_record(record_type='html', content=fig2.to_html(
        full_html=False, include_plotlyjs='cdn'))

    # rename cols to match user table
    forecast = forecast.rename({'ds': ds_col_name,
                                'yhat': '%s_forecast' % y_col_name}, axis=1)

    return forecast[[ds_col_name, '%s_forecast' % y_col_name, 'yhat_lower', 'yhat_upper']]
