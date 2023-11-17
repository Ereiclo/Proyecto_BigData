# import missing imports
from pyspark.sql import SparkSession
import numpy as np
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


spark = SparkSession.builder.appName(
    'recommendations').getOrCreate()


# create pyspark schema


def convert_to_df(rdd):
    parallel_rows = []
    row_id = 0

    for rdd_row in rdd:

        # iter features of rdd_row
        # print(rdd_row.features.toArray())
        for index, value in enumerate(rdd_row.features.toArray()):
            if value != 0:
                parallel_rows.append((row_id, index, float(value)))

        row_id += 1

    return spark.createDataFrame(parallel_rows, ["user", "item", "rating"])


# Create a Spark session
# Define the path to the LIBSVM file
libsvm_path = "./utility2_version2.txt"

# Read the LIBSVM file into a DataFrame speficy number of features with spark
data = spark.read.format("libsvm").option(
    "numFeatures", 1131).load(libsvm_path)
# data = spark.read.format("libsvm").load(libsvm_path)

# get rows of data
df = convert_to_df(data.collect())


# Build the recommendation model using ALS on the training data
df.show()

als = ALS(rank=80, maxIter=5, alpha=0.8, implicitPrefs=True, seed=0,
          userCol="user", itemCol="item", ratingCol="rating")
model = als.fit(df)


# query df for user 0
# df.filter(df.user == 0).show()

a = model.userFactors.collect()[19].features
print(len(model.itemFactors.collect()))
print(len(model.userFactors.collect()))

real_predictions = model.transform(df)
predictions = sorted(real_predictions.filter(
    df.user == 19).collect(), key=lambda r: r[1])

# i = 0

# for userdata in df.filter(df.user == 19).collect():
#     if userdata['item'] < len(model.itemFactors.collect()):
#         print(userdata)
#         print(predictions[i])
#         b = model.itemFactors.collect()[userdata["item"]].features
#         print(np.array(a).dot(np.array(b)))
#         i += 1


# print(model.recommendForUserSubset(df.filter(df.user == 19), 10).collect()[0].recommendations)

# print(b)
# print(np.array(a).dot(np.array(b)))

# Evaluate the model by computing the RMSE on the test data


# predictions = model.transform(df)

# predictions = sorted(predictions.collect(), key=lambda r: (r[0], r[1]))

# results = open("results.txt", "w")

# for pred in predictions[:1000]:
#     # results.write(str(pred['user']) + "," + str(pred['item']) +
#     #   "," + str(pred['prediction']) + "," + str(pred['rating']) + "\n")
#     # write to results with format: Usuario:user,item:item,prediction:prediction,rating:rating
#     results.write(
#         f"Usuario:{pred['user']},item:{pred['item']},prediction:{pred['prediction']},rating:{pred['rating']}\n")


# create file for results


# for userid, recomendation in model.recommendForAllUsers(10).collect()[:10]:
#     current_line = f"{userid}: "
#     for elem in recomendation:
#         current_line += str(elem["item"]) + "," + str(elem["rating"]) + "|"
#     current_line = current_line[:-1] + "\n"

#     results.write(current_line)


# results.close()


# results.write(str(userid) + "\n")
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",
        predictionCol="prediction")
rmse = evaluator.evaluate(real_predictions)
print("Root-mean-square error = " + str(rmse))


# print(row_matrix_rdd.take(10))


# iter rows and


# print(rows[:10])
