from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, split, expr, to_date, to_timestamp, date_format, lower, concat_ws, regexp_replace, when, regexp_replace, trim, regexp_extract, hour, mean, minute, lpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def airports_info_processing():
    """
    Processes airport information data, cleaning and converting specific columns to proper data types.
    N/A values are treated as null, and numeric fields are cast to their respective types.
    
    Returns:
        info_df (DataFrame): A Spark DataFrame with processed airport information.
    """
    # Initialize Spark Session with legacy time parser policy for compatibility
    spark = SparkSession.builder.appName("AirportsInfoDataProcessing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Define the schema for the airport information data
    schema = StructType([
        StructField("my_flightradar24_rating", StringType(), True),
        StructField("temp", StringType(), True),  # Placeholder for column due to scraping error
        StructField("arrival_delay_index", StringType(), True),
        StructField("departure_delay_index", StringType(), True),
        StructField("utc", StringType(), True),
        StructField("local", StringType(), True),
        StructField("airport", StringType(), True),
    ])

    # Load the data from a CSV file, ensuring correct schema application
    info_df = spark.read.csv('./data/history/airports_info.csv', schema=schema, header=False)

    # Drop the 'temp' column as it contains null values due to scraping errors
    info_df = info_df.drop("temp")

    # Replace "N/A" string values with null across the DataFrame
    info_df = info_df.na.replace("N/A", None)

    # Clean numeric fields and cast to correct types
    info_df = info_df.withColumn("my_flightradar24_rating", 
                                 regexp_replace(col("my_flightradar24_rating"), "[^0-9]", "").cast(IntegerType())) \
                     .withColumn("arrival_delay_index", col("arrival_delay_index").cast(FloatType())) \
                     .withColumn("departure_delay_index", col("departure_delay_index").cast(FloatType()))
    
    # Extract the utc time part and convert it to a Spark timestamp format
    info_df = info_df.withColumn("utc", to_timestamp(regexp_extract(col("utc"), "(\\d{2}:\\d{2})", 0), "HH:mm"))

    # Convert local time to a Spark timestamp format
    info_df = info_df.withColumn("local", to_timestamp(concat(lit("1970-01-01 "), col("local")), "yyyy-MM-dd hh:mm a"))

    # Calculate time difference utc-local
    info_df = info_df.withColumn("time_diff", col('utc')-col('local')).drop('utc', 'local')

    # Remove duplicates
    info_df = info_df.dropDuplicates()

    # Display the schema to verify data types
    for column_dtype in info_df.dtypes:
        print(column_dtype)

    # Display the processed DataFrame
    info_df.show(truncate=False)

    # Return the processed DataFrame
    return info_df





if __name__=="__main__":
    # Run the airports info processing function
    info_df = airports_info_processing()