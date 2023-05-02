#! Get titles of all movies that last more than 2 hours

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c
def task3(df):
    df = df.select(f.col(c.COLUMS_TITLE_BASICS[2]), f.col(c.COLUMS_TITLE_BASICS[3])). \
        where((f.col(c.COLUMS_TITLE_BASICS[7]) > 120)
              & (f.col(c.COLUMS_TITLE_BASICS[1]) == 'movie'))
    return df