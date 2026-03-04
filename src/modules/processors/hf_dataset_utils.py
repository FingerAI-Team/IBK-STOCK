from datasets import Dataset

def df_to_hfdata(df):
    return Dataset.from_pandas(df)