from src.config import dataConfig
import pandas as pd

config = dataConfig()

def data_to_df(dataset, columns):
    if isinstance(dataset, list):
        return pd.DataFrame(dataset, columns=columns)
    
def merge_data(df1, df2, how='inner', on=None):
    return pd.merge(df1, df2, how=how, on=on)

def filter_data(df, col, val):
    return df[df[col]==val].reset_index(drop=True)

def train_test_split(dataset, x_col, y_col):
    X, X_test, y, y_test = train_test_split(dataset[x_col], dataset[y_col], test_size=config.test_size, stratify=dataset[y_col], random_state=config.random_state)
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=config.val_test_size, stratify=y, random_state=config.random_state)
    return X_train, X_val, X_test, y_train, y_val, y_test  