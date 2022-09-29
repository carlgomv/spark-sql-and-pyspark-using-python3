# Source: https://sparkbyexamples.com/pyspark/pyspark-isnull/
#Import
import pyspark.sql.functions as F
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
    .appName("SparkByExamples.com").getOrCreate()

# Create DataFrame
data = [
    ("James",None,"M","  1120  "),
    ("Anna","NY","F"," 10   "),
    ("Julia",None,None,"  9223372036854775807"),
    ("Juan","CN","M","  "),
  ]

columns = ["name","state","gender","zip"]
df = spark.createDataFrame(data,columns)
print(f'Original Dataframe')
df.show()

print(f'Trim')
df = df.withColumn('zip',F.trim(df.zip))
df.show()
print(f'Filter Using isNotNull()')
df.filter(df.zip.isNotNull()).show()
print(f'cast long')
df = df.withColumn('zip',df.zip.cast('long'))
df.show()
print(f'na.fill')
df = df.na.fill(-1,subset=['zip']).show()
#df.filter(df.zip.cast('int').isNotNull()).show()

# # Using isNull()
# print(f'Filter Using is NULL - SQL')
# df.filter("state is NULL").show()
# print(f'Filter Using isNull()')
# df.filter(df.state.isNull()).show()

# from pyspark.sql.functions import col
# print(f'Filter Using col("state").isNull()')
# df.filter(col("state").isNull()).show()

# print(f'Filter Using state IS NULL AND gender IS NULL; both ways')
# df.filter("state IS NULL AND gender IS NULL").show()
# df.filter(df.state.isNull() & df.gender.isNull()).show()

# from pyspark.sql.functions import isnull
# print(f'Select using isnull(df.state)')
# df.select(isnull(df.state)).show()

# # Using isNotNull()
# from pyspark.sql.functions import col
# print(f'Filter using isNotNull()')
# df.filter("state IS NOT NULL").show()
# df.filter("NOT state IS NULL").show()
# df.filter(df.state.isNotNull()).show()
# df.filter(col("state").isNotNull()).show()

# print(f'na.drop state')
# df.na.fill(subset=["state"]).show()

# # Using pySpark SQL
# df.createOrReplaceTempView("DATA")
# print(f'Using pySpark SQL')
# spark.sql("SELECT * FROM DATA where STATE IS NULL").show()
# spark.sql("SELECT * FROM DATA where STATE IS NULL AND GENDER IS NULL").show()
# spark.sql("SELECT * FROM DATA where STATE IS NOT NULL").show()
