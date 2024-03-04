import pyspark
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, lit, current_date, expr, udf, lag, row_number, monotonically_increasing_id
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from datetime import datetime
import re

schema = StructType([
    StructField("PredictedDeparture", StringType(), False),
    StructField("Flight_ID", StringType(), False),
    StructField("Destination", StringType(), False),
    StructField("Airline", StringType(), False),
    StructField("Aircraft_ID", StringType(), False),
     StructField("Null", StringType(), False),
    StructField("TimeOfDeparture", StringType(), False),
    StructField("Date", StringType(), False)
   
   
])


spark = SparkSession.builder.appName('bovo').getOrCreate()
df_pyspark=spark.read.option('header','false').csv('outputa.csv',inferSchema=True, schema=schema)
df_pyspark.printSchema()
type(df_pyspark)
df_pyspark.show()
df_pyspark= df_pyspark.drop('Null')
df_pyspark=df_pyspark.na.drop(how="any", thresh=3)
df_pyspark.show(100)

# df_pyspark.write.option('header', 'true').csv('DataClean.csv')
df_pyspark.write.csv('DataClean', header=True)
