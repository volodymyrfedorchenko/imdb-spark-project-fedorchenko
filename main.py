#!
import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import settings as s
from read_write import read, write
import columns as c
import task1 as t1
import task2 as t2
import task3 as t3
import task4 as t4
import task5 as t5

findspark.init('c:/spark')
def main():

    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('imdb-spark-project-fedorchenko')
                     .config(conf=SparkConf())
                     .getOrCreate())

    #Test
    '''
    df_principals = spark_session.read.csv(
        s.TITLE_PRINCIPALS_PATH,
        schema=s.schema_title_principals,
        header=True,
        nullValue='null',
        sep='\t')
    df_principals = df_principals.filter(f.col(c.COLUMNS_TITLE_PRINCIPALS[3]) == 'actor')
    df_principals = df_principals.select(df_principals[c.COLUMNS_TITLE_PRINCIPALS[0]],
                                         df_principals[c.COLUMNS_TITLE_PRINCIPALS[2]],
                                         df_principals[c.COLUMNS_TITLE_PRINCIPALS[5]],)
    
    df_name = spark_session.read.csv(
        s.NAME_BASICS_PATH,
        schema=s.schema_name_basics,
        header=True,
        nullValue='null',
        sep='\t')
    df_name = df_name.select(df_name[c.COLUMS_NAME_BASICS[0]],
                             df_name[c.COLUMS_NAME_BASICS[1]])
    
    df_title_basics = spark_session.read.csv(
        s.TITLE_BASICS_PATH,
        schema=s.schema_title_basics,
        header=True,
        nullValue='null',
        sep='\t')
    df_title_basics = df_title_basics.select(df_title_basics[c.COLUMS_TITLE_BASICS[0]],
                                             df_title_basics[c.COLUMS_TITLE_BASICS[3]])

    #df_name.show(2000)
    #df_principals.show(10)
    #df_title_basics.show(10)

    df_principals_name = df_principals.join(df_name,
                                            str(c.COLUMNS_TITLE_PRINCIPALS[2]))
    #df_principals_name.show(10)

    df_principals_name_title = df_principals_name.join(df_title_basics,
                                                       str(c.COLUMNS_TITLE_PRINCIPALS[0]))

    df_name_title_principals = df_principals_name_title.select(str(c.COLUMS_NAME_BASICS[1]),
                                                               str(c.COLUMS_TITLE_BASICS[3]),
                                                               str(c.COLUMNS_TITLE_PRINCIPALS[5]))

    df_name_title_principals = df_name_title_principals.orderBy(str(c.COLUMS_NAME_BASICS[1]))

    df_name_title_principals.show(300)
    '''
    # Task 1
    '''
    df = read(spark_session, s.TITLE_AKAS_PATH, s.schema_title_akas)
    df = t1.task1(df)
    write(df, 'Task1')
    '''
    # Task 2
    '''
    df = read(spark_session, s.NAME_BASICS_PATH, s.schema_name_basics)
    df = t2.task2(df)
    write(df, 'Task2')
    '''
    # Task 3
    '''
    df = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)
    df = t3.task3(df)
    write(df, 'Task3')
    '''
    # Task 4
    '''
    df_principals = read(spark_session, s.TITLE_PRINCIPALS_PATH, s.schema_title_principals)
    df_name = read(spark_session, s.NAME_BASICS_PATH, s.schema_name_basics)
    df_title_basics = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)

    df = t4.task4(df_principals, df_name, df_title_basics)
    write(df, 'Task4')
    '''
    # Task 5

    df_title_akas = read(spark_session, s.TITLE_AKAS_PATH, s.schema_title_akas)
    df_title_basics = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)
    df_title_ratings = read(spark_session, s.TITLE_RATINGS_PATH, s.schema_title_ratings)

    df_title_akas_id_region = df_title_akas.filter(f.col(c.COLUMS_TITLE_AKAS[3]) != r'\N').\
                                            select(df_title_akas[c.COLUMS_TITLE_AKAS[0]],
                                                   df_title_akas[c.COLUMS_TITLE_AKAS[3]])

    df_title_basics_id_origtitle_adult = df_title_basics.select(df_title_basics[c.COLUMS_TITLE_BASICS[0]],
                                                                df_title_basics[c.COLUMS_TITLE_BASICS[3]],
                                                                df_title_basics[c.COLUMS_TITLE_BASICS[4]])

    df_title_ratings_id_rating = df_title_ratings.select(df_title_ratings[c.COLUMNS_TITLE_RATINGS[0]],
                                                                df_title_ratings[c.COLUMNS_TITLE_RATINGS[1]])

    df_title_basics_id_origtitle_adult_filter = df_title_basics_id_origtitle_adult.filter(f.col(c.COLUMS_TITLE_BASICS[4]) == 1)

    df_title_basics_id_origtitle_filter = df_title_basics_id_origtitle_adult.drop(f.col(c.COLUMS_TITLE_BASICS[4]))

    df_title_akas_id_region_origtitle_filter = df_title_akas_id_region.join(df_title_basics_id_origtitle_filter,
                                                                            df_title_akas_id_region[c.COLUMS_TITLE_AKAS[0]]
                                                                            == df_title_basics_id_origtitle_filter[c.COLUMS_TITLE_BASICS[0]])

    df_title_akas_id_region_origtitle_filter.show(truncate=False)

if __name__ == '__main__':
    main()

