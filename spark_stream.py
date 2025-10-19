
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StringType, IntegerType, LongType

schema = StructType()     .add("train_no", StringType())     .add("timestamp", LongType())     .add("station", StringType())     .add("status", StringType())     .add("delay_min", IntegerType())

spark = SparkSession.builder.appName("IRCTCStream").getOrCreate()

df = spark.readStream.format("kafka")     .option("kafka.bootstrap.servers", "localhost:9092")     .option("subscribe", "train_events")     .load()

parsed = df.selectExpr("CAST(value AS STRING) as json")     .select(from_json(col("json"), schema).alias("data"))     .select("data.*")

agg = parsed.groupBy("train_no").agg(avg("delay_min").alias("avg_delay_min"))

query = agg.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
