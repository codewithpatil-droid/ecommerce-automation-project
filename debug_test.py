 
import pandas as pd
import logging
from etl_pipeline import clean_data

logger = logging.getLogger()
df = pd.DataFrame({'quantity': [1.0, None, 3.0], 'revenue': [100.0, 200.0, None]})
print('Before:')
print(df)
result = clean_data(df, logger)
print('After:')
print(result)