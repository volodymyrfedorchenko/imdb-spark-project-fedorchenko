#! Get information about how many episodes in each TV Series.
# Get the top 50 of them starting from the TV Series with the biggest quantity of episodes.

from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c

def task6(df_title_basics, df_title_ratings_id_averageRating, df_title_episode):

    df_title_basics_tvSeries = df_title_basics.filter(f.col(c.COLUMS_TITLE_BASICS[1]) == 'tvSeries')
    df_title_basics_tvSeries_id_titleType_originalTitle = df_title_basics_tvSeries.select(
        df_title_basics[c.COLUMS_TITLE_BASICS[0]],
        df_title_basics[c.COLUMS_TITLE_BASICS[1]],
        df_title_basics[c.COLUMS_TITLE_BASICS[3]])

    df_title_episode = df_title_episode.select(df_title_episode[c.COLUMNS_TITLE_EPISODE[0]],
                                               df_title_episode[c.COLUMNS_TITLE_EPISODE[1]])

    df_title_basics_tvSeries_id_titleType_originalTitle_averageRating = \
        df_title_basics_tvSeries_id_titleType_originalTitle.join(df_title_ratings_id_averageRating,
                                                                 str(c.COLUMS_TITLE_BASICS[0]))

    df_title_basics_tvSeries_episode = \
        df_title_episode.join(df_title_basics_tvSeries_id_titleType_originalTitle_averageRating,
                              df_title_episode[c.COLUMNS_TITLE_EPISODE[1]]
                              == df_title_basics_tvSeries_id_titleType_originalTitle_averageRating \
                                                                                             [c.COLUMS_TITLE_BASICS[0]])

    df_episode_agg = df_title_basics_tvSeries_episode.groupBy(str(c.COLUMS_TITLE_BASICS[3])). \
                                                      agg({str(c.COLUMS_TITLE_BASICS[3]): 'count',
                                                           str(c.COLUMNS_TITLE_RATINGS[1]): 'max'}). \
                                                      orderBy([f"max({str(c.COLUMNS_TITLE_RATINGS[1])})",
                                                               f"count({str(c.COLUMS_TITLE_BASICS[3])})"],
                                                              ascending=[False, False])

    return df_episode_agg
