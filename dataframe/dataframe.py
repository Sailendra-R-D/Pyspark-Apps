# import required packages

from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Pyspark Application Started ...")
    # initializing SparkSession
    spark = SparkSession \
        .builder \
        .appName("zipct App") \
        .master("local[2]") \
        .getOrCreate()

    # Loading input csv file
    jobsDF = spark \
        .read \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .csv("nyc-jobs.csv")

    # prints schema of Data frame
    jobsDF.printSchema()

    # select only specific columns from data frame
    jobsDF \
        .select(jobsDF["Job ID"], jobsDF["Agency"], jobsDF["Posting Type"],
                jobsDF["# Of Positions"].alias("num_positions")) \
        .show(truncate=False)

    # get counts based on agency from data frame
    jobsDF \
        .groupBy("Agency") \
        .count().show(truncate=False)

    # stop the SparkSession object
    spark.stop()
    print("Pyspark Application Completed.")
