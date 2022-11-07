# Modelstar project

This is a [modelstar](https://modelstar.io) project created using a Snowflake project template.

## Add crendentials to your your project

Inside the project folder change the values in `modelstar.config.yaml` to the ones of your snowflake account information. 

## Start your modelstar session

```shell
modelstar use <session_name>
```

## Create a session and test the credectials with the configuration given in the modelstar.config.yaml

```shell
modelstar use <config_name>
``` 

## Register a forecasting function to your data warehouse

```shell
modelstar register forecast:univariate_time_series_forecast
```

## (Optional) Create a table to run this forecast function for.

Modelstar provides some sample data that you can create a table out of in your data warehouse to test this procedure.

```shell
modelstar create table sample_data/sales.csv:SALES
```