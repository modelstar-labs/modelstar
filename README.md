# Command line interface to work with modelstar

## Project initialization

```shell
modelstar init <project_name>/<.>
``` 

Creates a folder named as `<project_name>`. With a project template of the required files and folders. 

## Add snowflake credentials

Inside the project folder change the values in `modelstar_project.toml` to the ones of your snowflake account information. 

## Test connection with the credentails you've given

```shell
modelstar use <config_name>
``` 

## Register a python function to your database and schema

```shell
modelstar register addition.py addition
```