#! Path_of_files_and_schemas
import pyspark.sql.types as t
#Path_of_files
TITLE_AKAS_PATH = 'imdb-data/title.akas.tsv.gz'
NAME_BASICS_PATH = 'imdb-data/name.basics.tsv.gz'
TITLE_BASICS_PATH = 'imdb-data/title.basics.tsv.gz'
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

schema_name_basics = t.StructType([ \
        t.StructField('nconst', t.StringType(), True), \
        t.StructField('primaryName', t.StringType(), True), \
        t.StructField('birthYear', t.StringType(), True), \
        t.StructField('deathYear', t.StringType(), True), \
        t.StructField('primaryProfession', t.StringType(), True), \
        t.StructField('knownForTitles', t.StringType(), True) \
        ])

schema_title_basics = t.StructType([ \
        t.StructField('tconst', t.StringType(), True), \
        t.StructField('titleType', t.StringType(), True), \
        t.StructField('primaryTitle', t.StringType(), True), \
        t.StructField('originalTitle', t.StringType(), True), \
        t.StructField('isAdult', t.BooleanType(), True), \
        t.StructField('startYear', t.IntegerType(), True), \
        t.StructField('endYear', t.IntegerType(), True), \
        t.StructField('runtimeMinutes', t.IntegerType(), True), \
        t.StructField('genres', t.StringType(), True)
        ])

schema_title_episode = t.StructType([ \
        t.StructField('tconst', t.StringType(), True), \
        t.StructField('parentTconst', t.StringType(), True), \
        t.StructField('seasonNumber', t.IntegerType(), True), \
        t.StructField('episodeNumber', t.IntegerType(), True) \
        ])

