# Data Exploration and Manipulation
import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Numerical Computing
import numpy as np

# Interactive Data Visualization
import altair as alt
alt.data_transformers.enable("vegafusion")

# Ignore Optional Warnings
import warnings
warnings.filterwarnings('ignore')

# Load data from cryptocurrency csv file
crypto = pd.read_csv('/kaggle/input/crypto-and-stock-market-data-for-financial-analysis/cryptocurrency.csv')
# Load data from stocks csv file
stocks = pd.read_csv('/kaggle/input/crypto-and-stock-market-data-for-financial-analysis/stocks.csv')
# Information about Cryptocurrency
crypto.info()