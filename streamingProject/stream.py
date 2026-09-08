from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

spark = SparkSession.builder.appName('data streaming').master('local[*]').getOrCreate()

spark.sparkContext.setLogLevel("WARN")

input = 'streamingProject\\input'
output = 'streamingProject\\output'
checkpoint = 'streamingProject\\checkpoint'

schema = StructType(
    [
        StructField('customer_id',IntegerType(),True),
        StructField('name',StringType(),True),
        StructField('amount',IntegerType(),True)
    ]
)

df = (
    spark.readStream.format('csv').option('header','True').schema(schema).load(input)
    )

query = (
    df.writeStream.format('parquet').outputMode('append').option('checkpointLocation',checkpoint).option('path',output).start()
)

query.awaitTermination()