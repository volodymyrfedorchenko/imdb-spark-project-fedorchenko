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

if __name__ == '__main__':
    main()

