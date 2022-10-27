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

## Register the sample Python UDF

```shell
modelstar register function functions/find_capital.py:find_capital
```