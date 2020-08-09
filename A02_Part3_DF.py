# Databricks notebook source
# --------------------------------------------------------
#
# PYTHON PROGRAM DEFINITION
#
# The knowledge a computer has of Python can be specified in 3 levels:
# (1) Prelude knowledge --> The computer has it by default.
# (2) Borrowed knowledge --> The computer gets this knowledge from 3rd party libraries defined by others
#                            (but imported by us in this program).
# (3) Generated knowledge --> The computer gets this knowledge from the new functions defined by us in this program.
#
# When launching in a terminal the command:
# user:~$ python3 this_file.py
# our computer first processes this PYTHON PROGRAM DEFINITION section of the file.
# On it, our computer enhances its Python knowledge from levels (2) and (3) with the imports and new functions
# defined in the program. However, it still does not execute anything.
#
# --------------------------------------------------------

import pyspark
import pyspark.sql.functions
import pyspark.sql.functions as f
from pyspark.sql.functions import unix_timestamp, to_timestamp
from pyspark.sql.types import*
from datetime import datetime, date

# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(spark, my_dataset_dir):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
        ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
                        .option("delimiter", ";") \
                        .option("quote", "") \
                        .option("header", "false") \
                        .schema(my_schema) \
                        .load(my_dataset_dir)
    
    result = inputDF.count()
    print(result)
    
    pass

# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(spark, my_dataset_dir, station_number):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
        ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)
  
    #station_number = 240101
    station_date = inputDF.select(inputDF["station_number"], inputDF["date"])
    result = station_date.filter(station_date["station_number"] == station_number).distinct().count()
    print(result)
    pass

# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(spark, my_dataset_dir, station_number):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
        ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    station_times = inputDF.select(inputDF["station_number"], inputDF["scheduled_time"], inputDF["expected_arrival_time"])
    filtered_station= station_times.filter(station_times["station_number"] == station_number)
    first_result = filtered_station.filter(filtered_station["scheduled_time"] >= filtered_station["expected_arrival_time"])
    second_result = filtered_station.filter(filtered_station["scheduled_time"] < filtered_station["expected_arrival_time"])
    print((first_result.count(),second_result.count()))
    pass

# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(spark, my_dataset_dir, station_number):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
        ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    station_times = inputDF.select(inputDF["station_number"], inputDF["scheduled_time"], inputDF["day_of_week"])
    filtered_station= station_times.filter(station_times["station_number"] == station_number)
    days_grouped = filtered_station.groupBy(["day_of_week"]).agg({"scheduled_time": "collect_set"}) \
                                    .withColumnRenamed("collect_set(scheduled_time)", "bus_scheduled_time").collect()
    
    for i in days_grouped:
      print(i)
    pass

# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(spark, my_dataset_dir, station_number, month_list):
    # 1. We define the Schema of our DF.
    my_schema = pyspark.sql.types.StructType(
        [pyspark.sql.types.StructField("station_number", pyspark.sql.types.IntegerType(), True),
         pyspark.sql.types.StructField("station_name", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("direction", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("day_of_week", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("date", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("query_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("scheduled_time", pyspark.sql.types.StringType(), True),
         pyspark.sql.types.StructField("expected_arrival_time", pyspark.sql.types.StringType(), True)
        ])

    # 2. Operation C1: We create the DataFrame from the dataset and the schema
    inputDF = spark.read.format("csv") \
        .option("delimiter", ";") \
        .option("quote", "") \
        .option("header", "false") \
        .schema(my_schema) \
        .load(my_dataset_dir)

    station_times = inputDF.select(inputDF["station_number"], f.split(inputDF["date"],"/"), \
                                   inputDF["query_time"], inputDF["expected_arrival_time"], inputDF["day_of_week"]) \
                                  .withColumnRenamed("split(date, /)", "date")
    filtered_station= station_times.filter(station_times["station_number"] == station_number)
    filtered_dates = filtered_station.filter(filtered_station["date"][1].isin(month_list))
    add_drop_column = filtered_dates.withColumn("new_date", (f.concat(filtered_station["day_of_week"], \
                                                                      f.lit(" "),filtered_station["date"][1]))) \
                                      .drop("station_number","date","day_of_week")
    convert_to_time = add_drop_column.withColumn("expected_arrival_timestamp", to_timestamp(add_drop_column["expected_arrival_time"], \
                                                                                              "HH:mm:ss").cast(TimestampType())) \
                                      .withColumn("query_timestamp", to_timestamp(add_drop_column["query_time"], \
                                                                                            "HH:mm:ss").cast(TimestampType()))
    
    result = convert_to_time.withColumn("waiting_time", (f.col("expected_arrival_timestamp").cast(LongType()) - \
                                        f.col("query_timestamp").cast(LongType())))
    final_result = result.drop("expected_arrival_time","query_time","expected_arrival_timestamp","query_timestamp")\
                        .groupBy(["new_date"]).agg({"waiting_time": "avg"}).sort(f.col("avg(waiting_time)")).collect()
    for i in final_result:
      print(i)
    pass
#   

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(spark, my_dataset_dir, option):
    # Exercise 1:
    # Number of measurements per station number
    if option == 1:
        ex1(spark, my_dataset_dir)

    # Exercise 2: Station 240101 (UCC WGB - Lotabeg):
    # Number of different days for which data is collected.
    if option == 2:
        ex2(spark, my_dataset_dir, 240101)

    # Exercise 3: Station 240561 (UCC WGB - Curraheen):
    # Number of buses arriving ahead and behind schedule.
    if option == 3:
        ex3(spark, my_dataset_dir, 240561)

    # Exercise 4: Station 241111 (CIT Technology Park - Lotabeg):
    # List of buses scheduled per day of the week.
    if option == 4:
        ex4(spark, my_dataset_dir, 241111)

    # Exercise 5: Station 240491 (Patrick Street - Curraheen):
    # Average waiting time per day of the week during the Semester1 months. Sort the entries by decreasing average waiting time.
    if option == 5:
        ex5(spark, my_dataset_dir, 240491, ['09','10','11'])


# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. We use as many input arguments as needed
    option = 1

    # 2. Local or Databricks
    local_False_databricks_True = True

    # 3. We set the path to my_dataset and my_result
    my_local_path = "/home/nacho/CIT/Tools/MyCode/Spark/"
    my_databricks_path = "/"

    my_dataset_dir = "FileStore/tables/singlefile"

    if local_False_databricks_True == False:
        my_dataset_dir = my_local_path + my_dataset_dir
    else:
        my_dataset_dir = my_databricks_path + my_dataset_dir

    # 4. We configure the Spark Session
    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(spark, my_dataset_dir, option)
