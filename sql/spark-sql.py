# import required packages

import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import *

if __name__ == "__main__":
    print("Pyspark Application Started ...")
    # initializing SparkSession
    spark = SparkSession \
        .builder \
        .appName("movie lister app") \
        .master("local[2]") \
        .getOrCreate()


    # extracts year from title which is (title + year)
    def extractYearFromTitle(value):
        value = value.encode('utf-8')
        grp = re.search(r"\((\d+)\)(?!.*\((\d+)\))", value)
        year = -1
        if grp is not None:
            year = int(grp.group(1))
        return year


    # Loading movies csv file
    moviesDF = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("movies.csv")

    # creating a UDF for the extractYearFromTitle function created
    udfExtractYear = udf(extractYearFromTitle, IntegerType())
    # adding column release_year
    moviesDF_new = moviesDF.withColumn('release_year', udfExtractYear('title'))

    # filtering out records with invalid release_year
    moviesDF_new = moviesDF_new.filter(moviesDF_new['release_year'] > 0)

    # prints schema of movies data frame
    moviesDF_new.printSchema()

    # create a temp view for movies data frame
    moviesDF_new.createOrReplaceTempView("movies")

    # filtering movies based on genres passed
    spark.sql(
        "select title,release_year from movies where genres = 'Comedy|Drama'") \
        .show(truncate=False)

    # aggregating count of movies based on year and genre
    spark.sql(
        "select release_year,genres,count(*) as count \
            from movies group by release_year,genres order by release_year desc,count(*) desc") \
        .show(truncate=False)

    # aggregating top three genres of movies based on year
    spark.sql(
        "select release_year,genres,movies_count from \
            ( \
                select release_year,genres, count(*) as movies_count,\
                dense_rank() over (partition by release_year order by count(*) desc) as count_rank \
                from movies group by release_year,genres \
            ) A \
            where count_rank <=3 order by release_year desc,movies_count desc") \
        .show(truncate=False)

    # aggregating top movie genres based on year
    spark.sql(
        "select release_year,genres,movies_count from \
            ( \
                select release_year,genres, count(*) as movies_count,\
                dense_rank() over (partition by release_year order by count(*) desc) as count_rank \
                from movies group by release_year,genres \
            ) A \
            where count_rank=1 order by release_year desc,movies_count desc") \
        .show(truncate=False)

    # aggregating min and max movies released genres based on year
    spark.sql(
        "select release_year,sum(c) as total_movies_count,min(c) as min_movies_per_genres,max(c) as max_movies_per_genre \
            from \
                ( \
                        select release_year,genres,count(*) as c \
                        from movies \
                        group by release_year,genres \
                )X \
            group by release_year \
            order by release_year desc") \
        .show(truncate=False)

    # Loading tags csv file
    tagsDF = spark \
        .read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("tags.csv")

    # prints schema of movies data frame
    tagsDF.printSchema()

    # create a temp view for movies data frame
    tagsDF.createOrReplaceTempView("tags")

    # an example on join
    # retrieve tags associated with the movie title
    spark.sql(
        "select release_year,title, concat_ws(',',collect_set(trim(tag))) as tags \
            from \
                movies m \
            left join \
                tags t on m.movieId = t.movieId \
          group by release_year,title \
          order by release_year desc,title") \
        .show(truncate=False)

    # stop the SparkSession object
    spark.stop()
    print("Pyspark Application Completed.")
