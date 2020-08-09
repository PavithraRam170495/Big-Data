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
from datetime import datetime, date

# ------------------------------------------
# FUNCTION process_line
# ------------------------------------------
def process_line(line):
    # 1. We create the output variable
    res = ()

    # 2. We remove the end of line character
    line = line.replace("\n", "")

    # 3. We split the line by tabulator characters
    params = line.split(";")

    # 4. We assign res

    # ----------
    # Schema:
    # ----------
    # 0 -> station_number
    # 1 -> station_name
    # 2 -> direction
    # 3 -> day_of_week
    # 4 -> date
    # 5 -> query_time
    # 6 -> scheduled_time
    # 7 -> expected_arrival_time

    if (len(params) == 8):
        res = (int(params[0]),
               str(params[1]),
               str(params[2]),
               str(params[3]),
               str(params[4]),
               str(params[5]),
               str(params[6]),
               str(params[7])
               )

    # 5. We return res
    return res

# ------------------------------------------
# FUNCTION ex1
# ------------------------------------------
def ex1(sc, my_dataset_dir):
    inputRDD = sc.textFile(my_dataset_dir)
    resVAL = inputRDD.count()
    print(resVAL)
    pass

# ------------------------------------------
# FUNCTION ex2
# ------------------------------------------
def ex2(sc, my_dataset_dir, station_number):
    #station_number = 240101
    inputRDD = sc.textFile(my_dataset_dir)
    resVAL = inputRDD.map(process_line)
    dates = resVAL.map(lambda x: (x[4],x[0]))
    station = dates.filter(lambda x : x[1] == station_number).distinct().count()
    print(station)
    pass

# ------------------------------------------
# FUNCTION ex3
# ------------------------------------------
def ex3(sc, my_dataset_dir, station_number):
   
    inputRDD = sc.textFile(my_dataset_dir)
    resVAL = inputRDD.map(process_line)
    station_times = resVAL.map(lambda x: (x[0],x[-2],x[-1]))
    station = station_times.filter(lambda x : x[0] == station_number)
    result_one = station.filter(lambda x : x[1] >= x[2]).count()
    result_two = station.filter(lambda x : x[1] < x[2]).count()
    print((result_one,result_two))
    pass

# ------------------------------------------
# FUNCTION ex4
# ------------------------------------------
def ex4(sc, my_dataset_dir, station_number):
    
    inputRDD = sc.textFile(my_dataset_dir)
    resVAL = inputRDD.map(process_line)
    station = resVAL.filter(lambda x : x[0] == station_number)
    day_times = station.map(lambda x: (x[3],x[-2])).distinct()
    result = day_times.groupByKey()
    res = result.mapValues(lambda x : [i for i in x]).collect()
    for i in res:
      print(i)  
    pass

# ------------------------------------------
# FUNCTION ex5
# ------------------------------------------
def ex5(sc, my_dataset_dir, station_number, month_list):
    #month_list = ['09','10','11']
    inputRDD = sc.textFile(my_dataset_dir)
    resVAL = inputRDD.map(process_line)
    station = resVAL.filter(lambda x : x[0] == station_number)
    months = station.filter(lambda x : x[4][3:5] in(month_list))
    day_month = months.map(lambda x : (x[3],x[4][3:5],subtract_times(x[-1],x[-3])))
    result_format = day_month.map(lambda x : ((str(x[0])+" "+str(x[1])),x[2]))
    result = result_format.groupByKey()
    final_result = result.mapValues(lambda x : (sum(x)/len(x))).sortBy(lambda x : x[1]).collect()
    for i in final_result:
      print(i)
    pass

def subtract_times(eat,qt):
    qt = datetime.strptime(qt, '%H:%M:%S')
    eat = datetime.strptime(eat, '%H:%M:%S')
    time_difference = eat - qt
    return time_difference.total_seconds()
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(sc, my_dataset_dir, option):
    # Exercise 1:
    # Number of measurements per station number
    if option == 1:
        ex1(sc, my_dataset_dir)

    # Exercise 2: Station 240101 (UCC WGB - Lotabeg):
    # Number of different days for which data is collected.
    if option == 2:
        ex2(sc, my_dataset_dir, 240101)

    # Exercise 3: Station 240561 (UCC WGB - Curraheen):
    # Number of buses arriving ahead and behind schedule.
    if option == 3:
        ex3(sc, my_dataset_dir, 240561)

    # Exercise 4: Station 241111 (CIT Technology Park - Lotabeg):
    # List of buses scheduled per day of the week.
    if option == 4:
        ex4(sc, my_dataset_dir, 241111)

    # Exercise 5: Station 240491 (Patrick Street - Curraheen):
    # Average waiting time per day of the week during the Semester1 months. Sort the entries by decreasing average waiting time.
    if option == 5:
        ex5(sc, my_dataset_dir, 240491, ['09','10','11'])

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

    # 4. We configure the Spark Context
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel('WARN')
    print("\n\n\n")

    # 5. We call to our main function
    my_main(sc, my_dataset_dir, option)
