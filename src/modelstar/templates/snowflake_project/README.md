# Modelstar project

This is a [modelstar](https://modelstar.io) project created using a Snowflake project template.

## Quickstart

Initialize your modelstar project using

```shell
modelstar init project-name
```

## Add crendentials to your your project

Edit the `modelstar.toml` file inside your project.

```toml
[snowflake]

account = "<account>" 
username = "<username>" 
password = "<password>" 
role = "<role>"
database = "<database>" 
schema = "<schema>"
warehouse = "<warehouse>"
stage = "<stage>" 
```

## Start your modelstar session

```shell
modelstar use snowflake
```

## Register the sample Python UDF

```shell
modelstar register function functions/find_capital.py:find_capital
```

## Append yout `.gitignore` to setup for `modelstar`

Append the following into your git

```
# Modelstar
.modelstar/
modelstar.toml
```