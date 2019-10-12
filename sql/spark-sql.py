# import required packages

from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Pyspark Application Started ...")
    # initializing SparkSession
    spark = SparkSession \
        .builder \
        .appName("movie lister app") \
        .master("local[2]") \
        .getOrCreate()

    # Loading input csv file
    moviesDF = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("movies.csv")

    # prints schema of Data frame
    moviesDF.printSchema()

    # create a temp view for Data frame
    moviesDF.createOrReplaceTempView("movies")

    # filter movies based on genre
    spark.sql("select title from movies where genres = 'Comedy|Drama'").show(truncate=False)

    # stop the SparkSession object
    spark.stop()
    print("Pyspark Application Completed.")
