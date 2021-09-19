import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.ml.feature import PCA, VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
import pandas as pd
import numpy as np

def pca_apply(df_norm,k=2):
    '''
    Takes wide-format genome dataframe (direct or nromalized) and returns vectorized PCA components.
    Number of components ste via k.
    Prints variance explained by each component.
        
    :param df_norm: wide-format spark Dataframe (cell x gene). column names should be ['cell', gene names...]
        
    :return: df spark DataFrame with vector of PCA components- components
    '''
    # pre-propcess input cols
    col_dtypes = pd.DataFrame(df_norm.drop('cell').dtypes, columns=['col_name','dtype'])

    # Transformation pipeline
    string_cols    = col_dtypes[col_dtypes['dtype']=='string']['col_name'].tolist()
    numerical_cols = col_dtypes[col_dtypes['dtype']!='string']['col_name'].tolist()

    stages = []
    index_cols = []
    if string_cols: # empty list (no string cols) evaluates to False and skips below loop
        for col in string_cols:
            new_col = col + '_index'
            indexer = StringIndexer(inputCol=col, outputCol=new_col, handleInvalid='keep')
            stages.append(indexer)
            index_cols.append(new_col)
    
    print(f'Input has {len(numerical_cols)} numerical cols & {len(index_cols)} string cols.')

    assembler = VectorAssembler(inputCols=numerical_cols + index_cols, outputCol='vec_assemble')
    stages.append(assembler)
    pipe = Pipeline(stages=stages)
    pipe_model = pipe.fit(df_norm)

    df = pipe_model.transform(df_norm)
    df = df.select('cell','vec_assemble')

    # PCA
    pca = PCA(k=k, inputCol="vec_assemble", outputCol="components")
    model = pca.fit(df)
    print('PCA components:',model.getK())
    df = model.transform(df)
    print('explainedVariance')
    print('  By components:', model.explainedVariance)
    print('  Cumulative   :', np.cumsum(model.explainedVariance))
    print('  Total        :', sum(model.explainedVariance))
    
    df = df.select('cell','components')
    return df