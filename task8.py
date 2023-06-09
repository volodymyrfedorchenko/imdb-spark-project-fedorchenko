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
    df_title_titleType_originalTitle_genres_rating = df_title_id_titleType_originalTitle_genres_rating.select(
                                                                                               'titleType',
                                                                                               'originalTitle',
                                                                                               'averageRating',
                                                                                      f.explode('genres').alias('genr'))
    df_title_titleType_originalTitle_genres_rating_arating = \
        df_title_titleType_originalTitle_genres_rating.withColumn('antiRating', 10 - f.col('averageRating'))

    window = Window.partitionBy('genr').orderBy('antiRating')

    df_title_titleType_originalTitle_genres_rating_arating = \
        df_title_titleType_originalTitle_genres_rating_arating.withColumn('row_number', f.row_number().over(window))

    df_title_titleType_originalTitle_genres_rating_arating = \
        df_title_titleType_originalTitle_genres_rating_arating.filter(
                                    df_title_titleType_originalTitle_genres_rating_arating.row_number < 11).\
                                    orderBy(['genr', 'averageRating'], ascending=[False, False])

    df_title_titleType_originalTitle_genres_rating_arating = \
                        df_title_titleType_originalTitle_genres_rating_arating.filter(
                                                df_title_titleType_originalTitle_genres_rating_arating.genr != r'\N'). \
                                                select('genr', 'originalTitle', 'titleType', 'averageRating')

    return df_title_titleType_originalTitle_genres_rating_arating