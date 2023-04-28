#! Get names of people, corresponding movies/series and characters they played in those films.

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import columns as c
def task4(df_principals, df_name, df_title_basics):
    df_principals = df_principals.filter(f.col(c.COLUMNS_TITLE_PRINCIPALS[3]) == 'actor')

    df_principals = df_principals.select(df_principals[c.COLUMNS_TITLE_PRINCIPALS[0]],
                                         df_principals[c.COLUMNS_TITLE_PRINCIPALS[2]],
                                         df_principals[c.COLUMNS_TITLE_PRINCIPALS[5]], )

    df_name = df_name.select(df_name[c.COLUMS_NAME_BASICS[0]],
                             df_name[c.COLUMS_NAME_BASICS[1]])

    df_title_basics = df_title_basics.select(df_title_basics[c.COLUMS_TITLE_BASICS[0]],
                                             df_title_basics[c.COLUMS_TITLE_BASICS[3]])

    df_principals_name = df_principals.join(df_name,
                                            str(c.COLUMNS_TITLE_PRINCIPALS[2]))

    df_principals_name_title = df_principals_name.join(df_title_basics,
                                                       str(c.COLUMNS_TITLE_PRINCIPALS[0]))

    df_name_title_principals = df_principals_name_title.select(str(c.COLUMS_NAME_BASICS[1]),
                                                               str(c.COLUMS_TITLE_BASICS[3]),
                                                               str(c.COLUMNS_TITLE_PRINCIPALS[5]))

    df_name_title_principals = df_name_title_principals.orderBy(str(c.COLUMS_NAME_BASICS[1]))

    return df_name_title_principals