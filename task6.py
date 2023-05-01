#! Get information about how many episodes in each TV Series.
# Get the top 50 of them starting from the TV Series with the biggest quantity of episodes.

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c

def task6(df_title_akas, df_title_basics, df_title_ratings):
    pass
