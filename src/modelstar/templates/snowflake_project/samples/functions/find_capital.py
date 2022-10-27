'''
Test: 
modelstar register function /functions/example:find_capital
'''

from modelstar import modelstar_read_path
import pandas as pd

FILE_1 = modelstar_read_path(
    local_path='/Users/adithya/projects/modelstar-org/test/test-project/samples/data_functions/country-capitals.csv')


def find_capital(country_name: str) -> str:
    '''
    CREATE TABLE clients (
        client_id int,
        client_country string
    );

    INSERT INTO clients (client_id, client_country)
    VALUES
        (1, 'India'),
        (2, 'Germany'),
        (3, 'France'),
        (4, 'Spain'),
        (5, 'Italy'),
        (6, 'Canada'),
        (7, 'Mexico'),
        (8, 'Pakistan'),
        (9, 'Nepal');

    SELECT find_capital(client_country) as client_capital
    FROM clients;

    '''
    cinfo = pd.read_csv(FILE_1)

    country_details = cinfo.loc[cinfo['CountryName'] == country_name]
    capital_name = country_details.iloc[0]['CapitalName']

    return capital_name


if __name__ == '__main__':
    print(find_capital('India'))
