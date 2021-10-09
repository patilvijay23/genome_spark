import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.ml.feature import MinHashLSH

from clustering_functions import cluster_data_prep

# take input data: filtered
df_melt_flt = spark.read.parquet('./interim/df_melt_flt/')

# convert to wide format
df_wide_flt = df_melt_flt.groupBy('cell').pivot('variable').agg(F.max('value'))

# vectorize for hashing input
df_clust = cluster_data_prep(df_wide_flt)
df_clust.select('cell','features').show(5)

# hashing model
mh = MinHashLSH(inputCol='features',outputCol='hashes', numHashTables=1)
model = mh.fit(df_clust)

# apply the hash model
df_hash = model.transform(df_clust)
df_hash.select('cell','hashes').show(10,False)

# number of clusters
df_hash.select('hashes').drop_duplicates().count()

# assign cluster ids
df_hash_clust_id = df_hash.select('hashes').drop_duplicates().withColumn(
    'prediction', F.row_number().over(Window.orderBy('hashes')))
df_hash = df_hash.join(df_hash_clust_id, on=['hashes'],how='left')

df_hash.select('hashes','prediction').drop_duplicates().show()