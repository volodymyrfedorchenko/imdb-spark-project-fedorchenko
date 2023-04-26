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


if __name__ == '__main__':
    main()

