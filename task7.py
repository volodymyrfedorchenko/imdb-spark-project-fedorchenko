#! Get 10 titles of the most popular movies/series etc. by each decade.

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c

def task7(df_title_ratings_id_averageRating, df_title_basics):

    df_title_id_averageRating = df_title_ratings_id_averageRating.drop(f.col(c.COLUMNS_TITLE_RATINGS[2]))
    df_title_id_titleType_originalTitle_startYear = df_title_basics.select(df_title_basics[c.COLUMS_TITLE_BASICS[0]],
                                                                 df_title_basics[c.COLUMS_TITLE_BASICS[1]],
                                                                 df_title_basics[c.COLUMS_TITLE_BASICS[3]],
                                                                 df_title_basics[c.COLUMS_TITLE_BASICS[5]])
    df_title_id_titleType_originalTitle_startYear_rating = df_title_id_titleType_originalTitle_startYear.\
                                                                        join(df_title_id_averageRating,
                                                                            str(c.COLUMS_TITLE_BASICS[0]))
    df_title_id_titleType_originalTitle_startYear_rating_decade = df_title_id_titleType_originalTitle_startYear_rating.\
                                 withColumn('decade', f.format_string('%s%s - %s',
                                             (f.col(str(c.COLUMS_TITLE_BASICS[5])).cast('string')).substr(startPos = 0, length = 3),
                                             f.lit('0'),f.lit('9')))

    df_id_titleType_originalTitle_startYear_rating_arating_decade = \
        df_title_id_titleType_originalTitle_startYear_rating_decade.withColumn('antiRating', 10-f.col('averageRating'))

    window = Window.partitionBy('decade').orderBy('antiRating')

    df_id_titleType_originalTitle_startYear_rating_arating_decade = \
                    df_id_titleType_originalTitle_startYear_rating_arating_decade.withColumn('row_number',
                                                                                            f.row_number().over(window))
    df_id_titleType_originalTitle_startYear_rating_arating_decade = \
        df_id_titleType_originalTitle_startYear_rating_arating_decade.filter(
                                    df_id_titleType_originalTitle_startYear_rating_arating_decade.row_number < 11).\
                                    orderBy(['decade', 'averageRating'], ascending=[False, False])

    df_id_titleType_originalTitle_startYear_rating_arating_decade = \
        df_id_titleType_originalTitle_startYear_rating_arating_decade.filter(
            df_id_titleType_originalTitle_startYear_rating_arating_decade.startYear.cast('string') != r'\N').\
                  select('decade', 'originalTitle', 'titleType', 'averageRating')

    return df_id_titleType_originalTitle_startYear_rating_arating_decade