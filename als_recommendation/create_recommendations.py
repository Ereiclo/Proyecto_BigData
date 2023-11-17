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


def recommend(file_path, rank=80, maxIter=6, alpha=1, seed=0, best_items=10):
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
    users, df = convert_to_df(data.collect())

    # Build the recommendation model using ALS on the training data
    # df.show()

    als = ALS(rank=rank, maxIter=maxIter, alpha=alpha, implicitPrefs=True, seed=seed,
              userCol="user", itemCol="item", ratingCol="rating")
    model = als.fit(df)

    num_users = df.select("user").distinct().count()

    n = best_items

    if not os.path.exists(RECOMENDATIONS_RESULT_PATH):
        os.makedirs(RECOMENDATIONS_RESULT_PATH)

    # get file name of file_path with os
    filename = os.path.basename(file_path)
    print(filename)

    final_path = RECOMENDATIONS_RESULT_PATH + "/" + filename + ".als"

    results = open(final_path, "w", encoding="utf-8")

    print("Creando recomendaciones...")

    progress_bar = tqdm(total=num_users, unit='clientes')

    for user_id in range(num_users):

        # get items for user_id

        items_for_user = map(lambda x: x['item'],
                             df.filter(df.user == user_id).collect())

        # get items not for user_id
        items_not_scored = map(
            lambda x: (user_id, x),
            list(set(range(num_items)) - set(items_for_user)))

        # get predictions for items not scores
        predictions = map(
            lambda x: (x['item'], x['prediction']),
            model.transform(
                spark.createDataFrame(items_not_scored, ["user", "item"])).where((~ isnan(col('prediction')) & (col('prediction') >= 0))).collect())

        predictions = sorted(predictions, key=lambda x: x[1], reverse=True)

        # predictions = list(
        # filter(lambda x: not math.isnan(x[1]) and x[1] >= 0, predictions))

        results.write(f"{int(users[user_id])}: ")

        line = ""
        for item, prediction in predictions[:n]:
            # check if prediction is nan
            line += f"{int(item)},{prediction}|"
            # print(f"{int(item)},{prediction}|")

        results.write(line[:-1])
        results.write("\n")
        progress_bar.update(1)
        # break

    results.close()
    return final_path
