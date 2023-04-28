#! Get information about how many adult movies/series etc. there are per region.
# Get the top 100 of them from the region with the biggest count to the region with the smallest one.

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c
def task5(df):
    df = df.select(f.col(c.COLUMS_TITLE_BASICS[2]), f.col(c.COLUMS_TITLE_BASICS[3])). \
        where((f.col(c.COLUMS_TITLE_BASICS[6]) > 120)
              & (f.col(c.COLUMS_TITLE_BASICS[1]) == 'movie'))
    return df