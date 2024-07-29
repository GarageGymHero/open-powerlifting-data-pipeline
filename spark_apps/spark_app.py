from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, to_date

import argparse

def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('--input_path', required=True)
    parser.add_argument('--output_path', required=True)

    args = parser.parse_args()

    input_path = args.input_path
    output_path = args.output_path

    #Initialize Spark Session
    spark = SparkSession.builder \
        .appName('FilterData') \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.instances", "4") \
        .getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Convert string date to DateType
    df = df.withColumn("Date", to_date(df["Date"], "yyyy-MM-dd"))

    # Extract year and month from Date column
    df = df.withColumn("Year", year(df["Date"]))

    #Perform transformations
    transformed_df = df.drop('Wilks', 'Glossbrenner', 'Goodlift') \
        .filter(df['Federation'] == 'USAPL') \
        .filter(df['Sex'] == 'M') \
        .filter(df['Event'] == 'SBD') \
        .filter(df['equipment'] == 'Raw')
        
    # Write partitioned data back to GCS
    df.write \
        .partitionBy("Year") \
        .mode("overwrite") \
        .parquet(output_path)
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()