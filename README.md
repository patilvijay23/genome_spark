# genome_spark

## genome_analysis.py
Working code that uses built modules to analyse input cell x gene matrix.
Dataframes at interim steps are saved to disk as parquet files to avoid re-runing from start.

## data_load.py
1. read_pandas(): read input cell x gene matrix csv as pandas dataframe, melt it to get cell-gene as rows and convert both dataframes to spark df.
2. read_spark(): read input cell x gene matrix csv as spark dataframe, melt it to get cell-gene as rows. Also writes the two dfs to disk as parquet format so that this function need to be called again.

## data_filter.py
Creates `analyses` directory for writing qc outputs.
1. qc_matrix_cells(): summarize qc metrics per cell and write as csv.
2. qc_matrix_genes(): summarize qc metrics per gene and write as csv.
3. filter_matrix(): apply cell level, gene level or both filters to input cell-gene tall-format df and return filtered tall-format df.

## data_normalize.py
Two methods to normalize the input, filtered data
1. norm_quantile(): Takes tall-format genome dataframe and returns quantile normalized, wide-format genes matrix dataframe. Quantile normalization done withtout reference distribution and the values are set to avg. at each rank.
2. norm_global(): Takes tall-format genome dataframe and returns global normalized, wide-format genes matrix dataframe. Global normalization is similar to converting to CPM. It divides each gene value for a cell by sum of all values for that cell. This is then multiplied by scaling factor (10,000) and we take log(1+v).
