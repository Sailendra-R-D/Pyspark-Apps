# import required packages

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

if __name__ == "__main__":
    print("Pyspark Application Started ...")
    # initializing SparkSession
    spark = SparkSession \
        .builder \
        .appName("zipct App") \
        .master("local[2]") \
        .getOrCreate()

    schema = StructType([
        StructField("job_id", IntegerType(), True),
        StructField("agency", StringType(), True),
        StructField("posting_type", StringType(), True),
        StructField("num_positions", IntegerType(), True),
        StructField("business_title", StringType(), True),
        StructField("civil_service_title", StringType(), True),
        StructField("title_code no", StringType(), True),
        StructField("level", StringType(), True),
        StructField("job_category", StringType(), True),
        StructField("salary_range_from", DoubleType(), True),
        StructField("salary_range_to", DoubleType(), True),
        StructField("salary_frequency", StringType(), True),
        StructField("work_location", StringType(), True),
        StructField("division_work_unit", StringType(), True),
        StructField("job_description", StringType(), True),
        StructField("minimum_qual_requirements", StringType(), True),
        StructField("preferred_skills", StringType(), True),
        StructField("additional_information", StringType(), True),
        StructField("to_apply", StringType(), True),
        StructField("hours_shift", StringType(), True),
        StructField("work_location_1", StringType(), True),
        StructField("recruitment_contact", StringType(), True),
        StructField("residency_requirement", StringType(), True),
        StructField("posting_date", StringType(), True),
        StructField("post_until", StringType(), True),
        StructField("posting_updated", StringType(), True),
        StructField("process_date", StringType(), True)
    ])

    # Loading input csv file
    jobsDF = spark \
        .read \
        .option("header", "true") \
        .csv("nyc-jobs.csv", schema=schema)

    # prints schema of Data frame
    # jobsDF.printSchema()

    # create a temp view for Data frame
    jobsDF.createOrReplaceTempView("jobs")

    # select only specific columns from a temp view
    spark.sql("select job_id,agency,posting_type,num_positions from jobs").show()

    # get counts based on agency from a temp view
    spark.sql("select agency,count(agency) as count from jobs group by agency").show()

    # stop the SparkSession object
    spark.stop()
    print("Pyspark Application Completed.")
