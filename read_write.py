#!

def read(spark_session, path, schema=None):
    df = spark_session.read.csv(
        path,
        schema=schema,
        header=True,
        nullValue='null',
        sep='\t')
    return df


def write(df, directory_to_write=None):
    df.show(30, truncate=False)
    #df.write.csv(directory_to_write,
    #             header=True,
    #             sep='\t'
    #             )

