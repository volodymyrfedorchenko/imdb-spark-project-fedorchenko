#! Get 10 titles of the most popular movies/series etc. by each decade.

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c

def task7(df_title_basics, df_title_ratings_id_averageRating, df_title_episode):
    pass