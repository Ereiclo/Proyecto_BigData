# import missing imports
from tqdm import tqdm
from definitions.path import RECOMENDATIONS_RESULT_PATH
import os
import math
from pyspark.sql import SparkSession
import numpy as np
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql.functions import col, udf, isnan
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


def get_loss(file_path, rank=100, maxIter=6, alpha=1, seed=0):
    spark = SparkSession.builder.appName(
        'recommendations').getOrCreate()

    # create pyspark schema

    def convert_to_df(rdd):
        parallel_rows = []
        row_id = 0
        users = []

        for rdd_row in rdd:

            # iter features of rdd_row
            # print(rdd_row.features.toArray())
            users.append(rdd_row.label)
            for index, value in enumerate(rdd_row.features.toArray()):
                if value != 0:
                    parallel_rows.append((row_id, index, float(value)))

            row_id += 1

        return users, spark.createDataFrame(parallel_rows, ["user", "item", "rating"])

    # Create a Spark session
    # Define the path to the LIBSVM file
    libsvm_path = file_path

    # Read the LIBSVM file into a DataFrame speficy number of features with spark
    num_items = 1131
    data = spark.read.format("libsvm").option(
        "numFeatures", num_items).load(libsvm_path)

    # get rows of data
    _, df = convert_to_df(data.collect())

    # Build the recommendation model using ALS on the training data
    df.show()

    als = ALS(rank=rank, maxIter=maxIter, alpha=alpha, implicitPrefs=False, seed=seed,
              userCol="user", itemCol="item", ratingCol="rating")
    model = als.fit(df)

    predictions = model.transform(df)

    evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
                                    predictionCol="prediction")
    rmse = evaluator.evaluate(predictions)

    return rmse
