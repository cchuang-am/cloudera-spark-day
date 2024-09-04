import numpy as np
import pandas as pd

import dask.dataframe as dd
import dask.array as da
import dask.bag as db

from dsk.config import core_config

def load_customers():
    files = f"{core_config.DATA_SOURCE()}/customers"
    df = pd.read_parquet(files)
    return dd.from_pandas(df)