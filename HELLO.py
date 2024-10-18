import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, first
from pyspark.sql.functions import col,sum

# Correcting the Python executable path
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Venkat Jaswanth\Documents\Python\Python\Python37\python.exe"

# Create Spark Session
spark = SparkSession.builder \
    .appName("Example") \
    .master("local[*]") \
    .getOrCreate()


data = [("John Doe", "john@example.com", 50000.0),
    ("Jane Smith", "jane@example.com", 60000.0),
    ("Bob Johnson", "bob@example.com", 55000.0)]


schema="Name string,email string,salary double"
df=spark.createDataFrame(data,schema)
df.show()
"""
Task - Write a Spark code snippet to calculate the sum of a column in a DataFrame
"""
# Aggregate the sum of the salary column
total_salary = df.agg(sum(col("salary")).alias("total_salary")).first()[0]

print(f"Total salary: {total_salary}")