# Command line interface to work with modelstar

## Installation

```shell
pip install mdoelstar
```

## Project initialization

```shell
modelstar init <project_name>
``` 

Creates a folder named as `<project_name>`. With a project template of the required files and folders. 

## Add snowflake credentials

Inside the project folder change the values in `modelstar.config.yaml` to the ones of your snowflake account information. 

## Test connection with the credentails you've given

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