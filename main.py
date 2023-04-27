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

findspark.init('c:/spark')
def main():

    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('imdb-spark-project-fedorchenko')
                     .config(conf=SparkConf())
                     .getOrCreate())

    #Test
    '''
    df1 = spark_session.createDataFrame(
        [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
        ('time', 'id', 'v'))
    df1.write.csv('Test',
                  header=True,
                  mode='overwrite'
                  )
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

    df_name.show(10)
    df_principals.show(10)
    df_title_basics.show(10)

    df_principals_name = df_principals.join(df_name,
                                            str(c.COLUMNS_TITLE_PRINCIPALS[2]))
    df_principals_name.show(10)



if __name__ == '__main__':
    main()

