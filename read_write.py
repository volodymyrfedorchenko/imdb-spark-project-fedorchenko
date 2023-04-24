#!
import settings as s
def read(spark_session, path, schema=None):
    name_basics_df = spark_session.read.csv(
        path,
        schema=schema,
        header=True,
        nullValue='null',
        sep='\t')
    name_basics_df.show()


def write(df, directory_to_write):
    df.write.csv(directory_to_write, header=True)
    return
