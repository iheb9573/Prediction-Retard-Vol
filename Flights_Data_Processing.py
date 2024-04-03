from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, split, expr, to_date, to_timestamp, date_format, lower, concat_ws, regexp_replace, when, regexp_replace, trim, regexp_extract, hour, mean, minute, lpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def flights_processing():
    """
    Transforms flight data by cleaning and structuring. Removes unnecessary columns, normalizes dates and times, 
    extracts key information from strings, and filters based on flight status. Assumes data is loaded from a CSV 
    with a predefined schema.

    Returns:
        flights_df (DataFrame): A Spark DataFrame with processed flights information.
    """
    # Initialize Spark Session
    spark = SparkSession.builder.appName("FlightsDataProcessing")\
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
        .getOrCreate()

    # Define the schema for reading the CSV file
    schema = StructType([
        StructField("airport", StringType(), True),
        StructField("time", StringType(), True),
        StructField("flight_code", StringType(), True),
        StructField("destination", StringType(), True),
        StructField("airline", StringType(), True),
        StructField("aircraft", StringType(), True),
        StructField("color", StringType(), True),
        StructField("status", StringType(), True),
        StructField("date", StringType(), True),
        StructField("type", StringType(), True)
    ])

    # Load the data
    flights_df = spark.read.csv('./data/history/flights.csv', schema=schema, header=False)

    # Data Preprocessing Steps

    # 1. Remove unnecessary columns
    flights_df = flights_df.drop("color")

    # 2. Append year to 'date' and convert to DateType
    flights_df = flights_df.withColumn("date", concat(col("date"), lit(" 2024")))
    flights_df = flights_df.withColumn("date", to_date("date", "EEEE MMM dd yyyy"))

    # 3. Convert 'time' to TimestampType assuming it contains AM/PM
    # Concatenate 'date' with 'time' before converting to timestamp for 'expected_time'
    # This ensures the timestamp includes the correct date instead of defaulting to '1970-01-01'
    flights_df = flights_df.withColumn(
        "expected_time", 
        to_timestamp(concat_ws(" ", date_format(col("date"), "yyyy-MM-dd"), col("time")), "yyyy-MM-dd hh:mm a")
    )

    # 4. Extract city from 'destination' and convert it to lowercase
    flights_df = flights_df.withColumn("dest_city", split(col("destination"), " \\(")[0])

    # 5. Extract airport code from 'destination'
    flights_df = flights_df.withColumn("destination", lower(split(col("destination"), " \\(")[1].substr(0, 3)))

    # 6. Extract aircraft model from 'aircraft'
    flights_df = flights_df.withColumn("aircraft", split(col("aircraft"), " \\(")[0])

    # 7. Split 'status' into new 'status' and 'actual_time'
    split_col = split(col("status"), " ")
    flights_df = flights_df.withColumn("actual_time", expr("substring(status, length(split(status, ' ')[0]) + 2)"))
    flights_df = flights_df.withColumn("status", split_col.getItem(0))

    # 8. Filter rows to only include statuses 'Departed' or 'Arrived'
    flights_df = flights_df.filter(col("status").rlike("Departed|Arrived"))

    # 9. Correct 'actual_time' to match the 'date' column and convert to TimestampType
    flights_df = flights_df.withColumn("actual_time", to_timestamp(concat_ws(" ", col("date"), col("actual_time")), "yyyy-MM-dd hh:mm a")).drop("time", "date")

    # Add a new column 'rounded_hour' that represents the closest hour
    flights_df = flights_df.withColumn("date", to_date("expected_time")) \
        .withColumn("hour", hour("expected_time")) \
        .withColumn("minute", minute("expected_time")) \
        .withColumn("rounded_hour",
                        when(col("minute") >= 30, expr("hour + 1"))
                        .otherwise(col("hour"))
                    ) \
        .drop("hour", "minute")
    
    # Adjust for the case where adding 1 to the hour results in 24
    flights_df = flights_df.withColumn("rounded_hour",
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
    flights_df = flights_df.withColumn("rounded_hour", to_timestamp(datetime_str, "yyyy-MM-dd HH:mm:ss")).drop('date')

    # 10. Remove duplicates
    flights_df = flights_df.dropDuplicates()

    for i in flights_df.dtypes:
        print(i)
    # Display the processed DataFrame
    flights_df.show(truncate=False)

    # Return the processed DataFrame
    return flights_df


# Run the flights processing function
flights_df = flights_processing()