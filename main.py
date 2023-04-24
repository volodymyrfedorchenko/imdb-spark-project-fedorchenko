#!
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

import settings as s
from read_write import read, write
import task1 as t1


def main():

    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('imdb-spark-project-fedorchenko')
                     .config(conf=SparkConf())
                     .getOrCreate())

    read(spark_session, s.TITLE_AKAS_PATH)

if __name__ == '__main__':
    main()

