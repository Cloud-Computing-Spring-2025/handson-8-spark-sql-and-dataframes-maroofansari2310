from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lower, trim, desc

spark = SparkSession.builder.appName("HashtagTrends").getOrCreate()

# Load posts data (use absolute path)
posts_df = spark.read.option("header", True).csv("/input/posts.csv")

# Process hashtags
hashtag_counts = (
    posts_df
    .withColumn("Hashtag", explode(split(col("Hashtags"), ",")))
    .withColumn("Hashtag", lower(trim(col("Hashtag"))))
    .filter(col("Hashtag") != "")
    .groupBy("Hashtag")
    .count()
    .orderBy(desc("count"))
    .limit(10)
)

# Save result
hashtag_counts.coalesce(1).write.mode("overwrite").csv("/outputs/task1_hashtag_trends.csv", header=True)