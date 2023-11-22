import pandas as pd
data = pd.read_csv('https://raw.githubusercontent.com/Rohan-11-11/Aidetic-Data-Engineer-Assignment/main/database.csv')
print(data)

from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://rohan:password@localhost:3306/taskdb")

data.to_sql('neic_earthquakes', con=engine, if_exists='replace', index=False)

from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

mysql_properties = {
    "url": "jdbc:mysql://localhost:3306/taskdb",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "rohan",
    "password": "password"
}

# Read data from MySQL table
mysql_table_name = "neic_earthquakes"
df = spark.read.format("jdbc").\
    option("url", mysql_properties["url"]).\
    option("driver", mysql_properties["driver"]).\
    option("dbtable", mysql_table_name).\
    option("user", mysql_properties["user"]).\
    option("password", mysql_properties["password"])\
    .option('inferSchema','true')\
    .option('header','true')\
    .load()

df = df.withColumn("Date", to_date("Date","MM/dd/yyyy"))

## Q1.
# Extract day of the week from the date column
df1 = df.withColumn("day_of_week", dayofweek("Date"))
# Group by day of the week and calculate the average number of earthquakes
res1 = df1.groupBy("day_of_week").agg({"Magnitude": "count"}).orderBy("day_of_week")
res1.show()


## Q2. What is the relation between Day of the month and Number of earthquakes that happened in a year?
# Extract day of the month and year from the date column
df2 = df.withColumn("day_of_month", dayofmonth("Date")).withColumn("year", year("Date"))
# Group by year and day of the month and calculate the count of earthquakes
res2 = df2.groupBy("year", "day_of_month").agg({"magnitude": "count"}).orderBy("year", "day_of_month")
res2.show()


## Q3.What does the average frequency of earthquakes in a month from the year 1965 to 2016 tell us?
# First we have to Filter data for the years 1965 to 2016
df3 = df2.filter((df2["year"] >= 1965) & (df2["year"] <= 2016))
# Group by year and month, then calculate the average frequency of earthquakes
res3 = df3.groupBy("year", "day_of_month").agg({"magnitude": "count"}).groupBy("day_of_month").agg({"count(magnitude)": "avg"}).orderBy("day_of_month")
res3.show()


## Q4. What is the relation between Year and Number of earthquakes that happened in that year?
# Group by year and calculate the count of earthquakes for each year
res4 = df3.groupBy("year").agg({"magnitude": "count"}).orderBy("year")
res4.show()

## Q5. How has the earthquake magnitude on average been varied over the years?
#Group by year and calculate the average magnitude for each year
res5 = df2.groupBy("year").agg({"magnitude": "avg"}).orderBy("year")
res5.show()

## Q6.How does year impact the standard deviation of the earthquakes?
# Group by year and calculate the standard deviation of magnitudes for each year
res6 = df2.groupBy("year").agg({"magnitude": "stddev"}).orderBy("year")
res6.show()

## Q7. Does geographic location have anything to do with earthquakes?
# Group by geographic location (latitude and longitude) and calculate the average magnitude
res7 = df2.groupBy("latitude", "longitude").agg({"magnitude": "avg"})
res7.show()

## Q8. Where do earthquakes occur very frequently?
# Group by geographic location (latitude and longitude) and calculate the count of earthquakes
res8 = df2.groupBy("latitude", "longitude").agg({"magnitude": "count"}).orderBy(col("count(magnitude)").desc())
res8.show()

## Q9. What is the relation between Magnitude, Magnitude Type , Status and Root Mean Square of the earthquakes?
# Group by Magnitude, Magnitude Type, and Status, and calculate the average Root Mean Square
res9 = (
    df.groupBy("Magnitude", "Magnitude Type", "Status")
    .agg(avg("Root Mean Square").alias("Avg_RMS"))
    .orderBy("Magnitude", "Magnitude Type", "Status")
)
res9.show()

spark.stop()
