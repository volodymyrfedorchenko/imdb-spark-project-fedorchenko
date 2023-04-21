#!
from pyspark import SparkConf
from pyspark.sql import SparkSession

def main():

    spark_session = (SparkSession.builder()
                     .master('local')
                     .appName('imdb-spark-project-fedorchenko')
                     .config(conf=SparkConf())
                     .getOrCreate())

    name_basics_df = spark_session.read.csv(D:/Fedor/Project/imdb-spark-project-fedorchenko/imdb-data/name.basics.tsv.gz)
    name_basics_df.show()

if __name__ == '__main__':
    main()

