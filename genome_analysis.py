# working file for using the genome_spark modules to analyze genome data
# dataframes are written to disk at various stages: use these to skip starting steps when required by loading pre-processed df

import pyspark.sql.functions as F
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from data_load import read_spark, read_spark_t
from data_filter import qc_matrix_cells, qc_matrix_genes, filter_matrix
from data_normalize import norm_quantile, norm_global
from dimension_reduction import pca_apply, umap_apply, tsne_apply
from clustering_functions import cluster_data_prep, k_means_spark

path_to_file = "./data/brain_counts.csv"

# transpose to gene x cell to manage memory requirement
df = pd.read_csv(path_to_file, index_col=0)
df = df.transpose()
df.to_csv(path_to_file.split('.csv')[0]+'_t.csv')

# load csv data (update path_to_file to transposed csv if required)
path_to_file = "./data/brain_counts_t.csv"
df, df_melt = read_spark_t(path_to_file)

# or, read pre-loaded data written to disk in the read_spark/read_spark_t fns
df= spark.read.parquet('./interim/df/')
df_melt = spark.read.parquet('./interim/df_melt/')

# qc cells & write csv to `analyses` folder
df_qc_cells = qc_matrix_cells(df_melt)
df_qc_cells.toPandas().to_csv('./analyses/qc_cells.csv', index=False)

# qc genes & write csv to `analyses` folder
df_qc_genes = qc_matrix_genes(df_melt)
df_qc_genes.toPandas().to_csv('./analyses/qc_genes.csv', index=False)

# filter the genome data using output from the above two qc fns
df_melt_flt = filter_matrix(df_melt,df_qc_cells,df_qc_genes)

# write filtered df_melt to drive
df_melt_flt.write.parquet('./interim/df_melt_flt/', mode='overwrite')
df_melt_flt = spark.read.parquet('./interim/df_melt_flt/')

# count check
{'cells':df_melt_flt.select('cell').drop_duplicates().count(),
'genes':df_melt_flt.select('variable').drop_duplicates().count()}

# Normalization: output is wide-format
df_norm = norm_quantile(df_melt_flt)

# write norm df to drive
df_norm.write.parquet('./interim/df_norm/', mode='overwrite')
df_norm = spark.read.parquet('./interim/df_norm/')

# count check
{'cells (row count)':df_norm.count(),
'genes (column count)':len(df_norm.columns)}

# df_norm_g = norm_global(df_melt_flt)
# df_norm_g.write.parquet('./interim/df_norm_g/', mode='overwrite')
# df_norm_g = spark.read.parquet('./interim/df_norm_g/')

df_norm = df_norm.persist()
df_norm.count()

# PCA- output df has the PCA components as a Vector
df_pca = pca_apply(df_norm,k=50)

df_pca.write.parquet('./interim/df_pca/', mode='overwrite')
df_pca = spark.read.parquet('./interim/df_pca/')

# UMAP
df_umap = umap_apply(df_norm, n_components=50)

df_umap.write.parquet('./interim/df_umap/', mode='overwrite')
df_umap = spark.read.parquet('./interim/df_umap/')

# t-sne
df_tsne = tsne_apply(df_norm, n_components=50)

df_tsne.write.parquet('./interim/df_tsne/', mode='overwrite')
df_tsne = spark.read.parquet('./interim/df_tsne/')

# Join with metadata: use the below logic to join any of the previous dfs with metadata as required
## Load
df_metadata = spark.read.csv('./data/brain_metadata.csv', header=True, inferSchema=True)

cols_rename = [cols.replace('.','_').replace(' ','_').replace('(','_').replace(')','_') for cols in df_metadata.columns]
df_metadata = df_metadata.toDF(*cols_rename)

df_metadata = df_metadata.withColumn('cell',F.regexp_replace(F.col('cell'), '[.\s()]', '_'))

## join
df_pca = df_pca.join(df_metadata, on=['cell'],how='left')

# df_tsne = df_tsne.join(df_metadata, on=['cell'],how='left')


# Visualization

## indexes for components to visualize
## - starts at 0; x_comp=0 and y_comp=1 plots first component on x-axis and second component on y-axis
## - update as required

x_comp = 0
y_comp = 1

# extract components- PCA (only required for PCA as PCA output is Spark Vector)
df_plot = df_pca.toPandas()

df_plot['x_comp'] = df_plot['components'].apply(lambda x: x[x_comp])
df_plot['y_comp'] = df_plot['components'].apply(lambda x: x[y_comp])

# plot PCA: update the 'hue' column as required, or skip it if not required
plt.figure(figsize=(15,15))
sns.scatterplot(x=df_plot['x_comp'], y=df_plot['y_comp'], hue=df_plot['subtissue'])
plt.show()

# extract components- UMAP/t-sne: update the select statement to skip/update the coloring col (subtissue)
df_plot = df_tsne.select(['cell',str(x_comp), str(y_comp),'subtissue']).toPandas()

# plot UMAP/t-sne
plt.figure(figsize=(15,15))
sns.scatterplot(x=df_plot[str(x_comp)], y=df_plot[str(y_comp)], hue=df_plot['subtissue'])
plt.show()


# clustering- takes Vector assembled column of features to perform clustering
# (use the cluster_prep fn in case the input features aren't Vector assembled already- only PCA is already vectorized)




##
# QC checks on the data flow
# take data for selected cells and genes from df_melt and compare against values in df_qc_*
cells_to_qc = ['D1.MAA000560.3_10_M.1.1','D18.MAA000935.3_8_M.1.1']
genes_to_qc = ['Rnaseh2b','Suclg1']

df_qc = df_melt.where((F.col('cell').isin(cells_to_qc)) | (F.col('variable').isin(genes_to_qc)))
df_qc.toPandas().to_csv('./analyses/qc_flow_checks.csv', index=False)