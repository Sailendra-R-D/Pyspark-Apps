# import required packages

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if __name__ == "__main__":
    print("HelloWorld Pyspark Application Started ...")
    # initializing SparkSession
    spark = SparkSession \
        .builder \
        .appName("PySpark App") \
        .master("local[2]") \
        .getOrCreate()

    # Loading input json file
    pokemonsDF = spark \
        .read \
        .option("inferSchema", "true") \
        .json("Pokédex.json", multiLine=True)

    pokemonsDF = pokemonsDF \
        .select(F
                .explode(pokemonsDF.pokemon)
                .alias('pokemon')) \
        .select('pokemon.*')

    pokemonsDF.printSchema()
    pokemonsDF.show(truncate=False)

    # stop the SparkSession object
    spark.stop()
    print("HelloWorld Pyspark Application Completed.")
