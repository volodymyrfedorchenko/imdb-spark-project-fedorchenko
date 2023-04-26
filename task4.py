#! Get names of people, corresponding movies/series and characters they played in those films.

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c
def task4(df):
    df = df.select(f.col(c.COLUMS_TITLE_BASICS[2]), f.col(c.COLUMS_TITLE_BASICS[3])). \
        where((f.col(c.COLUMS_TITLE_BASICS[6]) > 120)
              & (f.col(c.COLUMS_TITLE_BASICS[1]) == 'movie'))
    return df