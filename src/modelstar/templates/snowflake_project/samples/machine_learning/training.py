from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import pandas as pd
from modelstar import modelstar_write_path


def ad_sales_model(features_df: pd.DataFrame) -> dict:

    # create a Python list of feature names
    feature_cols = ['TV', 'RADIO', 'NEWSPAPER']

    # use the list to select a subset of the original DataFrame
    X = features_df[feature_cols]

    # select a Series from the DataFrame
    y = features_df['SALES']

    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=1)

    model = LinearRegression()
    model.fit(X_train.values, y_train)

    # Save the model to a local file

    model_path = modelstar_write_path(
        local_path='/Users/adithya/projects/modelstar-org/test/test-project/models/model_sproc_v1.joblib', write_object=model)

    return {"Model path": model_path}


if __name__ == '__main__':
    # Load the data
    data = pd.read_csv('Advertising.csv', index_col=0)
    ad_sales_model(data)
