#! Get 10 titles of the most popular movies/series etc. by each genre

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c

def task8(df_title_ratings_id_averageRating, df_title_basics):

    df_title_id_averageRating = df_title_ratings_id_averageRating.drop(f.col(c.COLUMNS_TITLE_RATINGS[2]))
    df_title_id_titleType_originalTitle_genres = df_title_basics.select(df_title_basics[c.COLUMS_TITLE_BASICS[0]],
                                                                           df_title_basics[c.COLUMS_TITLE_BASICS[1]],
                                                                           df_title_basics[c.COLUMS_TITLE_BASICS[3]],
                                                                           df_title_basics[c.COLUMS_TITLE_BASICS[8]])

    df_title_id_titleType_originalTitle_genres = df_title_id_titleType_originalTitle_genres.withColumn(
                                                                                        'genres', f.split('genres',','))
    df_title_id_titleType_originalTitle_genres_rating = df_title_id_titleType_originalTitle_genres. \
                                                                           join(df_title_id_averageRating,
                                                                                   str(c.COLUMS_TITLE_BASICS[0]))
    df_title_id_titleType_originalTitle_genres_rating.show(30, truncate=False)