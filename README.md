# genome_spark
Built using Spark 3.1.2

## genome_analysis.py
Working code that uses built modules to analyze input cell x gene matrix.
Dataframes at interim steps are saved to disk as parquet files to avoid re-running from start.

This repo contains brain_counts.csv in gzip compressed format but the codes use the uncompressed format as input. Please extract the csv file from the gzip file.
Data file source: https://github.com/chanzuckerberg/scRNA-python-workshop/blob/master/content/data.zip

## data_load.py
1. `read_pandas()`: read input cell x gene matrix csv as pandas dataframe, melt it to get cell-gene as rows and convert both dataframes to spark df.
2. `read_spark()`: read input cell x gene matrix csv as spark dataframe, melt it to get cell-gene as rows. Also writes the two dfs to disk as parquet format so that this function need to be called again.
3. `read_spark_t()`: read input gene x cell matrix csv as spark dataframe, melt it to get cell-gene as rows. Also writes the two dfs to disk as parquet format so that this function need to be called again.

## data_filter.py
Creates `analyses` directory for writing qc outputs.
1. `qc_matrix_cells()`: summarize qc metrics per cell and write as csv.
2. `qc_matrix_genes()`: summarize qc metrics per gene and write as csv.
3. `filter_matrix()`: apply cell level, gene level or both filters to input cell-gene tall-format df and return filtered tall-format df.

## data_normalize.py
Two methods to normalize the input, filtered data
1. `norm_quantile()`: Takes tall-format genome dataframe and returns quantile normalized, wide-format genes matrix dataframe. Quantile normalization done without reference distribution and the values are set to avg. at each rank.
2. `norm_global()`: Takes tall-format genome dataframe and returns global normalized, wide-format genes matrix dataframe. Global normalization is similar to converting to CPM. It divides each gene value for a cell by sum of all values for that cell. This is then multiplied by scaling factor (10,000) and we take log(1+v).

## dimension_reduction.py
Two methods for dimensionality reduction.
1. `pca_apply()`: Uses spark implementation of PCA. Prints variance of individual components, cumulative variance and total variance and returns PCA transformed df. Printed variances can be used for visualization of explained variance, or the function can be updated to return the variance as as array.
Extract required PCA components from the output Vector to plot them.
2. `umap_apply()`: Uses python implementation of UMAP as it is not available in Spark. Input is wide format (cell x gene) spark df and output is wide format cell x components spark df.
3. `tsne_apply()`: Uses python sklearn implementation of t-sne as it is not available in Spark. Input is wide format (cell x gene) spark df and output is wide format cell x components spark df.

## clustering_functions.py
Functions for data prep and k-means clustering.

## differential_expression.py
t-test based selection of top variables that are different between chosen cluster and rest of the data.

# Requirements for Python
Create a Python 3.8 environment using the requirements.txt file.

Commands for conda:
> `conda create -n genome python=3.8`

Switch to the env
> `conda activate genome`

Install dependencies
> `pip install -r requirements.txt`