from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
import copy

# Create a Spark session
spark = SparkSession.builder.appName(
    'video_games_review').config("spark.driver.memory", "15g").getOrCreate()

# Create a DataFrame
data = [(0, 0, 4.0), (0, 1, 2.0), (1, 1, 3.0), (1, 2, 4.0), (2, 1, 1.0), (2, 2, 5.0)]
df = spark.createDataFrame(data, ["user", "item", "rating"])

# Train ALS model
als = ALS(rank=10, maxIter=5, seed=0,implicitPrefs=True, regParam=0.01, alpha=1.0)
model = als.fit(df)

# Print model rank
print("Model Rank:", model.rank)

# Show user factors
user_factors = model.userFactors.orderBy("id").collect()
print("User Factors:")
for row in user_factors:
    print(row)
