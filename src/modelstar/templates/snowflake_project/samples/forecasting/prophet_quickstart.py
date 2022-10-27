from pprint import pprint
import pandas as pd
from prophet import Prophet

df = pd.read_csv(
    '/Users/adithya/projects/modelstar-org/demos/prophet/forecasting/example_wp_log_peyton_manning.csv')

pprint(df.head())
pprint(df.tail())

m = Prophet()
m.fit(df)


future = m.make_future_dataframe(periods=365)
pprint(future.head())
pprint(future.tail())

# forecast = m.predict(future)
# pprint(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail())

# # fig1 = m.plot(forecast)