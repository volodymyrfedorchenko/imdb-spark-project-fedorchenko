#! Get information about how many adult movies/series etc. there are per region.
# Get the top 100 of them from the region with the biggest count to the region with the smallest one.

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c
def task5(df_title_akas, df_title_basics, df_title_ratings):
    # 5_0
    df_title_akas_id_region = df_title_akas.filter(f.col(c.COLUMS_TITLE_AKAS[3]) != r'\N').\
        select(df_title_akas[c.COLUMS_TITLE_AKAS[0]],
               df_title_akas[c.COLUMS_TITLE_AKAS[3]])

    df_title_basics_id_origtitle_adult = df_title_basics.select(df_title_basics[c.COLUMS_TITLE_BASICS[0]],
                                                                df_title_basics[c.COLUMS_TITLE_BASICS[3]],
                                                                df_title_basics[c.COLUMS_TITLE_BASICS[4]])

    df_title_ratings_id_rating = df_title_ratings.select(df_title_ratings[c.COLUMNS_TITLE_RATINGS[0]],
                                                         df_title_ratings[c.COLUMNS_TITLE_RATINGS[1]])

    df_title_basics_id_origtitle_adult_filter = df_title_basics_id_origtitle_adult.\
                                                                           filter(f.col(c.COLUMS_TITLE_BASICS[4]) == 1)

    df_title_basics_id_origtitle_filter = df_title_basics_id_origtitle_adult_filter.drop(f.col(c.COLUMS_TITLE_BASICS[4]))

    df_title_akas_id_region_origtitle_filter = df_title_akas_id_region.\
                                                                       join(df_title_basics_id_origtitle_filter,
                                                                            df_title_akas_id_region[c.COLUMS_TITLE_AKAS[0]]
                                                                            == df_title_basics_id_origtitle_filter[c.COLUMS_TITLE_BASICS[0]])

    df_tconst_region_originalTitle = df_title_akas_id_region_origtitle_filter.select(str(c.COLUMS_TITLE_BASICS[0]),
                                                                                     str(c.COLUMS_TITLE_AKAS[3]),
                                                                                     str(c.COLUMS_TITLE_BASICS[3]))
    # 5_1
    df_region_count = df_tconst_region_originalTitle.groupBy(str(c.COLUMS_TITLE_AKAS[3])).count()

    df_tconst_region_originalTitle_count = df_tconst_region_originalTitle.join(df_region_count,
                                                                               str(c.COLUMS_TITLE_AKAS[3]))

    df_tconst_region_originalTitle_count_rating = df_tconst_region_originalTitle_count.join(df_title_ratings_id_rating,
                                                                                        str(c.COLUMS_TITLE_BASICS[0]))

    window = Window.partitionBy('region').orderBy(['count', 'region', 'averageRating'])

    df_tconst_region_originalTitle_count_rating = df_tconst_region_originalTitle_count_rating.\
                                                                   withColumn('row_number', f.row_number().over(window))

    df_tconst_region_originalTitle_count_rating = df_tconst_region_originalTitle_count_rating.\
                                                 filter(df_tconst_region_originalTitle_count_rating.row_number < 101).\
                                                 orderBy(['count', 'region', 'averageRating'],
                                                          ascending=[False, False, False])

    df_tconst_region_originalTitle_count_rating = df_tconst_region_originalTitle_count_rating.\
                                                             select('region', 'count', 'originalTitle', 'averageRating')

    return df_tconst_region_originalTitle_count_rating