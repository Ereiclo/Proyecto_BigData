from pyspark import SparkContext

from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql.functions import col, udf

sc = SparkContext("local", "LIBSVM to RowMatrix Example")


def convert_toRowMatrix(rdd):
    parallel_rows = []
    for rdd_row in rdd:
        parallel_rows.append(rdd_row.features)
    

    print(type(parallel_rows[0]))
    return RowMatrix(sc.parallelize(parallel_rows))


# Create a Spark session

# Specify the path to the LIBSVM file
libsvm_file_path = "./utility2.txt_redo"

# Read the LIBSVM file into a DataFrame
# data = spark.read.format("libsvm").load(libsvm_file_path)
data = MLUtils.loadLibSVMFile(sc, libsvm_file_path)




matrix = convert_toRowMatrix(data.collect())

print(matrix.numRows())


svd = matrix.computeSVD(1000, computeU=True)


# convert_toRowMatrix(data)

# Stop the Spark session
sc.stop()