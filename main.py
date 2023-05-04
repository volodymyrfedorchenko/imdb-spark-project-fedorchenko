#!
import findspark
from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
import pyspark.sql.types as t
import pyspark.sql.functions as f

import settings as s
from read_write import read, write, write_limit
import columns as c
import task1 as t1
import task2 as t2
import task3 as t3
import task4 as t4
import task5 as t5
import task6 as t6
import task7 as t7
import task8 as t8

findspark.init('c:/spark')
def main():

    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('imdb-spark-project-fedorchenko')
                     .config(conf=SparkConf())
                     .getOrCreate())


    # Task 1

    df = read(spark_session, s.TITLE_AKAS_PATH, s.schema_title_akas)
    
    df = t1.task1(df)
    write(df, 'Task1')

    # Task 2

    df = read(spark_session, s.NAME_BASICS_PATH, s.schema_name_basics)
    
    df = t2.task2(df)
    write(df, 'Task2')

    # Task 3

    df = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)
    
    df = t3.task3(df)
    write(df, 'Task3')

    # Task 4

    df_principals = read(spark_session, s.TITLE_PRINCIPALS_PATH, s.schema_title_principals)
    df_name = read(spark_session, s.NAME_BASICS_PATH, s.schema_name_basics)
    df_title_basics = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)

    df = t4.task4(df_principals, df_name, df_title_basics)
    write(df, 'Task4')    

    # Task 5

    df_title_akas = read(spark_session, s.TITLE_AKAS_PATH, s.schema_title_akas)
    df_title_basics = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)
    df_title_ratings = read(spark_session, s.TITLE_RATINGS_PATH, s.schema_title_ratings)

    df = t5.task5(df_title_akas, df_title_basics, df_title_ratings)
    write(df, 'Task5')

    # Task 6

    df_title_basics = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)
    df_title_ratings_id_averageRating = read(spark_session, s.TITLE_RATINGS_PATH, s.schema_title_ratings)                                                                           
    df_title_episode = read(spark_session, s.TITLE_EPISODE_PATH, s.schema_title_episode)

    df = t6.task6(df_title_basics, df_title_ratings_id_averageRating, df_title_episode)
    write_limit(df, 'Task6', 50)

    # Task 7

    df_title_ratings_id_averageRating = read(spark_session, s.TITLE_RATINGS_PATH, s.schema_title_ratings)
    df_title_basics = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)

    df = t7.task7(df_title_ratings_id_averageRating, df_title_basics)
    write(df, 'Task7')

    # Task 8

    df_title_ratings_id_averageRating = read(spark_session, s.TITLE_RATINGS_PATH, s.schema_title_ratings)
    df_title_basics = read(spark_session, s.TITLE_BASICS_PATH, s.schema_title_basics)

    df = t8.task8(df_title_ratings_id_averageRating, df_title_basics)
    write(df, 'Task8')

if __name__ == '__main__':
    main()

