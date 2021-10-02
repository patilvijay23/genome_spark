# UDF error 1
udf not working due to `org.apache.spark.SparkException: Python worker failed to connect back.`

Add environment variable
- `PYSPARK_PYTHON` = `python`

Restart all terminals for new env var to be used

Ref: https://stackoverflow.com/questions/56213955/python-worker-failed-to-connect-back-in-pyspark-or-spark-version-2-3-1


# UDF error 2
Error msg: `net.razorvine.pickle.PickleException: expected zero arguments for construction of ClassDict (for numpy.dtype)`

This is data type return issue.

If your UDF output is expected to be `FloatType()`, add `float()` to the return value of the function

Ref: https://stackoverflow.com/questions/61183355/pyspark-udf-for-converting-utm-error-expected-zero-arguments-for-construction-of

# UDF error unused solution
```sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
spark.conf.set("spark.sql.execution.pyspark.udf.simplifiedTraceback.enabled", True)
```
