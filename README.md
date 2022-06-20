# Parallelized_calculation_of_hourly_average_of_certain_air_pollutant_in_a_big_dataset_using_Spark
Parallelized calculation of hourly average of certain air pollutant in a big dataset using Spark, Fall 2021

Parallelized calculation of hourly average of certain air pollutant in a big dataset using Spark

•	Big dataset of sensor-measured air pollutants every five minutes for the year 2013 in the state of Texas 

•	Developed a PySpark solution that calculates hourly average of each pollutant for each site separately utilizing spark data-frames to serialize the structured data into off heap storage in binary format and performed several transformations on it

•	Utilized NoSQL database Cassandra and compared the performance and file size with utilizing the CSV, Parquet, and JSON file formats as the input dataset while varying number of spark executors

•	Skills used: Python, Apache PySpark, Apache Cassandra

This project was submitted as part of a Big Data Analytics course taught by Professor Edgar Gabriel at the University of Houston Fall 2021
