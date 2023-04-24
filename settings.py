#! Path_of_files_and_schemas


#Path_of_files
TITLE_AKAS_PATH = 'D:/Fedor/Project/imdb-spark-project-fedorchenko/imdb-data/title.akas.tsv.gz'
TITLE_EPISODE_PATH = 'D:/Fedor/Project/imdb-spark-project-fedorchenko/imdb-data/title.episode.tsv.gz'

# Schemas

schema_title_akas = t.StructType([ \
        t.StructField('titleId', t.StringType(), True), \
        t.StructField('ordering', t.IntegerType(), True), \
        t.StructField('title', t.StringType(), True), \
        t.StructField('region', t.StringType(), True), \
        t.StructField('language', t.StringType(), True), \
        t.StructField('types', t.List, True), \
        t.StructField('attributes', t.List, True), \
        t.StructField('isOriginalTitle', t.BooleanType(), True) \
        ])

schema_title_episode = t.StructType([ \
        t.StructField('tconst', t.StringType(), True), \
        t.StructField('parentTconst', t.StringType(), True), \
        t.StructField('seasonNumber', t.IntegerType(), True), \
        t.StructField('episodeNumber', t.IntegerType(), True) \
        ])