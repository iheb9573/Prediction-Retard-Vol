from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, split, expr, to_date, to_timestamp, date_format, lower, concat_ws, regexp_replace, when, regexp_replace, trim, regexp_extract, hour, mean, minute, lpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import AirportsInfo


def weather_processing():
    """
    Processes weather data by cleaning and transforming specific columns.
    This includes removing non-numeric characters, handling special cases in visibility,
    and converting date_time strings to timestamp format.

    Returns:
        weather_df (DataFrame): A Spark DataFrame with processed weather information.
    """
    # Initialize Spark Session with a specified app name and configuration
    spark = SparkSession.builder.appName("WeatherDataProcessing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Read the CSV file. Assuming the file path is './data/complex_weather.csv'
    weather_df = spark.read.format("text").load("./data/history/weather.csv")

    weather_df = weather_df.withColumn("day", expr("regexp_extract(value, 'Day: ([^,]+)', 1)"))
    weather_df = weather_df.withColumn("time", expr("regexp_extract(value, 'Time: ([^,]+)', 1)"))
    weather_df = weather_df.withColumn("wind_direction", expr("regexp_extract(value, 'Wind direction: ([^,]+)', 1)"))
    weather_df = weather_df.withColumn("wind_speed", expr("regexp_extract(value, 'Wind speed: ([^,]+)', 1)"))
    weather_df = weather_df.withColumn("temperature", expr("regexp_extract(value, 'Temperature: ([^,]+)', 1)"))
    weather_df = weather_df.withColumn("dew_point", expr("regexp_extract(value, 'Dew point: ([^,]+)', 1)"))
    weather_df = weather_df.withColumn("pressure", expr("regexp_extract(value, 'Pressure: ([^,]+)', 1)"))
    weather_df = weather_df.withColumn("visibility", expr("regexp_extract(value, 'Visibility: ([^,]+)', 1)"))
    weather_df = weather_df.withColumn("date_time", regexp_extract("value", "(\d{2}:\d{2}:\d{2} \d{4}-\d{2}-\d{2})", 0))
    weather_df = weather_df.withColumn("airport", regexp_extract(col("value"), ",([a-z]{3})$", 1))

    # Drop unnecessary columns
    weather_df = weather_df.drop("day", "time")

    # Clean numeric columns by removing non-numeric characters and casting to IntegerType
    numeric_columns = ["wind_direction", "wind_speed", "temperature", "dew_point", "pressure"]

    
    for column in numeric_columns:
        weather_df = weather_df.withColumn(column, when(col(column) != "", col(column)))
        weather_df = weather_df.withColumn(column, regexp_replace(col(column), "[^0-9]", "").cast(IntegerType()))
    
    # Handle special visibility cases by replacing specific text with a numeric value and cleaning
    weather_df = weather_df.withColumn("visibility",
                                        when(col("visibility").rlike("Sky and visibility OK"), 10000)
                                        .otherwise(regexp_replace(col("visibility"), "[^0-9]", ""))
                                        .cast(IntegerType()))

    # Convert 'date_time' column to timestamp format
    weather_df = weather_df.withColumn("date_time", to_timestamp(col("date_time"), "HH:mm:ss yyyy-MM-dd"))

    # Remove duplicates and value column
    weather_df = weather_df.dropDuplicates().drop('value')

    # Join the airports_info data with the aggregated weather data
    weather_df = weather_df.join(info_df, "airport", "left")

    # Converting weather date_time to local time using difference from joining info_df
    weather_df = weather_df.withColumn("date_time", expr("date_time - time_diff")).drop("time_diff")
    
    # Add a new column 'rounded_hour' that represents the closest hour
    weather_df = weather_df.withColumn("date", to_date("date_time")) \
        .withColumn("hour", hour("date_time")) \
        .withColumn("minute", minute("date_time")) \
        .withColumn("rounded_hour",
                        when(col("minute") >= 30, expr("hour + 1"))
                        .otherwise(col("hour"))
                    ) \
        .drop("hour", "minute")
    
    # Adjust for the case where adding 1 to the hour results in 24
    weather_df = weather_df.withColumn("rounded_hour",
                    when(col("rounded_hour") == 24, 0)
                    .otherwise(col("rounded_hour"))
                    )

    # Convert 'hour_column' to a string with two digits
    rounded_hour = lpad(col("rounded_hour"), 2, '0')
    
    # Concatenate 'date_column' and 'hour_str' to form a datetime string
    datetime_str = concat_ws(" ", col("date"), rounded_hour)

    # Append ":00:00" to represent minutes and seconds, forming a full datetime string
    datetime_str = concat_ws(":", datetime_str, lit("00"), lit("00"))

    # Convert the datetime string to a timestamp
    weather_df = weather_df.withColumn("rounded_hour", to_timestamp(datetime_str, "yyyy-MM-dd HH:mm:ss")).drop('date')
    
    # Aggregating wind direction, wind speed, temperature, dew point, pressure and visibility
    weather_df = weather_df.groupBy("airport", "rounded_hour").agg(
        mean("wind_direction").alias("wind_direction"),
        mean("wind_speed").alias("wind_speed"),
        mean("temperature").alias("temperature"),
        mean("dew_point").alias("dew_point"),
        mean("pressure").alias("pressure"),
        mean("visibility").alias("visibility"),
    )
    
    for i in weather_df.dtypes:
        print(i)
        
    # Display the processed DataFrame
    weather_df.show(100, truncate=False)

    # Return the processed DataFrame
    return weather_df

# Run the weather processing function
weather_df = weather_processing()


