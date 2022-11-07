# Modelstar project

This is a [modelstar](https://modelstar.io) project created using a Snowflake project template.

## Quickstart

Initialize your modelstar project using

```shell
modelstar init <project_name>
```

## Add crendentials to your your project

Edit the `modelstar.config.yaml` file inside your project to configure your datawarehouse.

## Start your modelstar session

```shell
modelstar use <session_name>
```

## Create a sample table

```shell
modelstar create table samples/functions/clients.csv:clients
```

## Register the sample Python UDF

```shell
modelstar register function samples/functions/find_capital.py:find_capital
```

## Run a sample query

```shell
modelstar run 'SELECT * FROM CLIENTS'
```

```shell
modelstar run 'SELECT CLIENT_ID, CLIENT_COUNTRY, find_capital(CLIENT_COUNTRY) as CLIENT_CITY FROM CLIENTS'
```

## Register a stored-procedure

```shell
modelstar register procedure samples/machine_learning/training.py:ad_sales_model
```