# MVP V1

-   [x] project setup
-   [x] cli interface
-   [x] snowflake connector
    -   [ ] test
-   [ ] sql parser
    -   [ ] https://github.com/andialbrecht/sqlparse
    -   [ ] https://github.com/tobymao/sqlglot (https://www.reddit.com/r/programming/comments/orezss/i_wrote_an_extremely_fast_sql_parser_in_python_it/)
    -   [ ] https://github.com/klahnakoski/mo-sql-parsing
    -   [ ] https://pglast.readthedocs.io/en/v3/
    -   [ ] PyParsing: https://github.com/pyparsing/pyparsing/blob/master/examples/select_parser.py
        -   [ ] https://stackoverflow.com/questions/1394998/parsing-sql-with-python
-   [ ] build model

## File handling

-   [x] Reading a file (let the user upload the file by themselves into the stage and provide the location to it in a modelstar_read_file() function)
    -   [x] put modelstar file and get stage location if modelstar-read... is called
    -   [x] import the snowflake_path files into the function imports
    -   [x] import the modelstar file into the function imports
    -   [x] implement the change paths using the modelstar_read...
    -   [ ] Maybe check if dependencies exist?

## Working with entire Tables as dataframes

-   [ ] Implement stored procedure from functions

# MVP V2

-   [ ] Upload the file and get the path to the stage automaticall when the function file has a file that is read.
