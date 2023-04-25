#! Path_of_files_and_schemas
import pyspark.sql.types as t
#Path_of_files
TITLE_AKAS_PATH = 'imdb-data/title.akas.tsv.gz'
TITLE_EPISODE_PATH = 'imdb-data/title.episode.tsv.gz'

# Schemas
schema_title_akas = t.StructType([ \
        t.StructField('titleId', t.StringType(), True), \
        t.StructField('ordering', t.IntegerType(), True), \
        t.StructField('title', t.StringType(), True), \
        t.StructField('region', t.StringType(), True), \
        t.StructField('language', t.StringType(), True), \
        t.StructField('types', t.StringType(), True), \
        t.StructField('attributes', t.StringType(), True), \
        t.StructField('isOriginalTitle', t.BooleanType(), True) \
        ])

schema_title_episode = t.StructType([ \
        t.StructField('tconst', t.StringType(), True), \
        t.StructField('parentTconst', t.StringType(), True), \
        t.StructField('seasonNumber', t.IntegerType(), True), \
        t.StructField('episodeNumber', t.IntegerType(), True) \
        ])