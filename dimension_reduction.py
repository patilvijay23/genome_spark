import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.ml.feature import PCA, VectorAssembler


def pca_apply(df_norm,k=2):
    '''
    Takes wide-format genome dataframe (direct or nromalized) and returns vectorized PCA components.
    Number of components ste via k.
    Prints variance explained by each component.
        
    :param df_norm: wide-format spark Dataframe (cell x gene). column names should be ['cell', gene names...]
        
    :return: df spark DataFrame with vector of PCA components- components
    '''
    # vector assemble input cols
    inputCols = df_norm.drop('cell').columns
    # TODO: string/numerics split and stringIndexer() here

    vec_assembler = VectorAssembler(inputCols = inputCols, outputCol='vec_assemble')
    df = vec_assembler.transform(df_norm)
    df = df.select('cell','vec_assemble')

    # PCA
    pca = PCA(k=k, inputCol="vec_assemble", outputCol="components")
    model = pca.fit(df)
    print('PCA components:',model.getK())
    df = model.transform(df)
    print('explainedVariance')
    print('  By components:', model.explainedVariance)
    print('  Total:', sum(model.explainedVariance))
    
    df = df.select('cell','components')