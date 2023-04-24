#!
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark.sql.functions as f

def main():

    spark_session = (SparkSession.builder
                     .master('local')
                     .appName('imdb-spark-project-fedorchenko')
                     .config(conf=SparkConf())
                     .getOrCreate())

    schema = StructType([ \
        StructField('tconst', StringType(), True), \
        StructField('parentTconst', StringType(), True), \
        StructField('seasonNumber', IntegerType(), True), \
        StructField('episodeNumber', IntegerType(), True) \
        ])
    name_basics_df = spark_session.read.csv('D:/Fedor/Project/imdb-spark-project-fedorchenko/imdb-data/title.episode.tsv.gz',
                                            schema=schema,
                                            header=True,
                                            nullValue='null',
                                            sep = '\t')
    name_basics_df.show()

if __name__ == '__main__':
    main()

