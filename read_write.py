#!
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
def read(spark_session, path, schema=None):
    df = spark_session.read.csv(
        path,
        schema=schema,
        header=True,
        nullValue='null',
        sep='\t')
    return df


def write(df, directory_to_write='Temp'):
    df.show(30, truncate=False)
    '''
    df.write.csv(directory_to_write,
                 header=True,
                 mode='overwrite'
                 )
    '''

def write_limit(df, directory_to_write='Temp', kol = '10'):
    df.show(30, truncate=False)
    '''
    df.write.option("maxRecordsPerFile", kol).csv(directory_to_write,
                                                  header=True,
                                                  mode='overwrite'
                                                  )
    '''
