#! Get the list of people’s names, who were born in the 19th century

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c
def task2(df):
    df = df.select(list(c.COLUMS_NAME_BASICS[1:3])) \
        .where(df[c.COLUMS_NAME_BASICS[2]].
               substr(0, 2) == '19')
    return df.select(df[c.COLUMS_NAME_BASICS[1]])

