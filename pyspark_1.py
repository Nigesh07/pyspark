# Databricks notebook source
# No need to import SparkSession or create the spark object in Databricks
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('nigesh').getOrCreate()
data=[(1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
       (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano'),
      (1,'nigesh'),(2,'praveen'),(3,'yogesh'),(4,'mano')
      ]
schema=['id','name']
df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('nigesh').getOrCreate()
df=spark.read.option('header',True).option('inferschems',True).csv('/Volumes/workspace/default/nigesh/car_prices.csv').display()

# COMMAND ----------

df = spark.read.csv("/Volumes/workspace/default/nigesh/car_prices.csv", header=True, inferSchema=True)
df.select("make").display()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("make")).display()

# COMMAND ----------

columns=[col for col in df.columns]
df.select(*columns).display()

# COMMAND ----------

df.select(df.columns).display()

# COMMAND ----------

df.select(df.columns[0:5]).display()

# COMMAND ----------

df.select([df["make"],df["model"]]).display()

# COMMAND ----------


df.select(df['make'],df['model']).display()

# COMMAND ----------

df.select(col('make')).display()

# COMMAND ----------

columns=[col for col in df.columns]
df.select(*columns).display()

# COMMAND ----------

df.select(df.columns).display()

# COMMAND ----------

df.select(df.columns[0:4]).display()

# COMMAND ----------

df.select([df["make"],df["model"]]).display()

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat_ws
df2=df.withColumn('total price',col('sellingprice')+lit(1000))
df2.select(df2.columns).display()

# COMMAND ----------

from pyspark.sql.functions import col,lit,concat_ws
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('nigesh').getOrCreate()
df=spark.read.option('header',True).option('inferschems',True).csv('/Volumes/workspace/default/nigesh/car_prices.csv')
df2=df.withColumn('total price',col('sellingprice')+lit(1000))
df2.select(df2.columns)
df2=df.withColumn('total price',col('sellingprice')+lit(1000))
df2=df.withColumn('total price',col('sellingprice')-lit(100))
df3=df2.withColumn('model',concat_ws(' ',col('make'),col('model')))
df4=df3.drop('make')
df5=df.collect()
print(df5)

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('nigesh').getOrCreate()
df=spark.read.option('header',True).option('inferschema',True).csv('/Volumes/workspace/default/nigesh/car_prices.csv')

# COMMAND ----------

df5=df.collect()
df6=df5[0][5]
print(df6)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType
from pyspark.sql.functions import col,concat_ws

spark=SparkSession.builder.appName('nigesh').getOrCreate()
data=[
    (1,[{"name":"nigesh","age":25}]),
       (2,[{"name":"niky","age":19}]),
       (3,[{"name":"nisha","age":20}]),
       (4,[{"name":"nisha","age":20}])
    ]
      
schema =StructType([
    StructField("id", IntegerType(), True),
    StructField("name", ArrayType(StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ])), True)
    ])

df=spark.createDataFrame(data,schema).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,startswith,endswith,contains
spark = SparkSession.builder.appName('nigesh').getOrCreate()
df = spark.read.option('header', True).option('inferschema', True).csv('/Volumes/workspace/default/nigesh/car_prices.csv')
##df2 = df.sort(col("year")== "2015")
##df2=df.limit(2)
# df2.select(col("year")=="2015")
# display(df2)
#df2=df.filter((col("year")==2015)&(col("make")=="Honda"))
#df2=df.filter(col("year").startswith("2"))
#df2=df.filter(col("year").endswith("0"))
df2=df.filter(col("year").contains("2012"))
df2.display()


# COMMAND ----------

# No need to import SparkSession or create the spark object in Databricks
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,ArrayType
spark=SparkSession.builder.appName('nigesh').getOrCreate()
data=[
    (1,'nigesh',19,'chennai'),
    (2,'niky',20,'madurai'),
    (1,'niky',19,'chennai'),
    (4,'praveen',22,'madurai'),
    (5,'mano',23,'chennai'),
      (1,'nigesh',19,'chennai'),
       (4,'praveen',22,'madurai'),
      ]
schema=StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("city",StringType(),True)
])
df=spark.createDataFrame(data,schema)
df2=df.dropDuplicates(["name"]).display()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("nigesh").getOrCreate()

# Correct CSV read
df = spark.read.option("header", True).option("inferSchema", True).csv("/Volumes/workspace/default/nigesh/car_prices.csv")

# Sort
df1=df.filter(col("year").isNotNull())
df_sorted = df1.orderBy(col("make").asc(),col("model").desc()).display()


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark = SparkSession.builder.appName("nigesh").getOrCreate()
data1=[(1,'nigesh',19,'chennai'),
       (2,'niky',20,'madurai'),]
data2=[(3,'nigesh',19,'chennai'),
       (4,'praveen',22,'madurai'),]
data3=[(5,'nigesh',20,'chennai'),
       (6,'yogesh',22,'madurai'),]
schema=StructType([
    StructField("id",IntegerType(),True),
    StructField("name",StringType(),True),
    StructField("age",IntegerType(),True),
    StructField("city",StringType(),True)
])
df1=spark.createDataFrame(data1,schema)
df2=spark.createDataFrame(data2,schema)
df3=spark.createDataFrame(data3,schema)
df4=df1.union(df2).union(df3)
df4=df4.dropDuplicates(["name"])
df4.display()
