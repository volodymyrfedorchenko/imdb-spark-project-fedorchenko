#!
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c
def task1(df):
    df = df.where((df[c.COLUMS_TITLE_AKAS[3]] == 'UA')
                            & (df[c.COLUMS_TITLE_AKAS[4]] == 'uk'))
    return df.select(df[c.COLUMS_TITLE_AKAS[2]])

