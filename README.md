# UoM_MapReduce-vs-Spark

This is a coursework prepared for the Module CS5229 - Big Data Analytics Technologies.
Author - Y. R Jayawardane - yasanka.24@cse.mrt.ac.lk

## Set up

Set up Amazon EMR cluster and S3 bucket, as we use EMR for data processing and S3 as the initial data storage.

## Terminal Access
Access to EMR terminal via putty, Unix terminal

## Spark app
After accessing the EMR terminal, open the spark application by running the following command.

```bash
spark-shell
```

### Load data from csv stored in S3 and create a temp view

```bash
val df = spark.read.format("csv").load("s3://flight-delay/Input/DelayedFlights-att1.csv")

df.show()

df.write.format("parquet").partitionBy("_c1").save("s3://flight-delay/Spark/DelayedFlights-Part");

val df2 = spark.read.format("parquet").load("s3://flight-delay/Spark/DelayedFlights-Part")

df2.show()

df2.createOrReplaceTempView("delay_flights")
```
###Calculations

#### Calculating Year wise carrier delay from 2003-2010
```bash
val result_part1 = spark.sql("SELECT _c1 AS Year, avg((_c25 /_c15)*100) avg_carrier_delay from delay_flights WHERE _c1 >= 2003 AND _c1 <= 2010 GROUP BY Year ORDER BY Year ASC")
spark.time(result_part1.show())
```

#### Calculating Year wise NAS delay from 2003-2010
```bash
val result_part1 = spark.sql("SELECT _c1 AS Year, avg((_c27 /_c15)*100) avg_nas_delay from delay_flights WHERE _c1 >= 2003 AND _c1 <= 2010 GROUP BY Year ORDER BY Year ASC")
```

#### Calculating Year wise Weather delay from 2003-2010
```bash
val result_part1 = spark.sql("SELECT _c1 AS Year, avg((_c26 /_c15)*100) avg_weather_delay from delay_flights WHERE _c1 >= 2003 AND _c1 <= 2010 GROUP BY Year ORDER BY Year ASC")
spark.time(result_part1.show())
```

#### Calculating Year wise late aircraft delay from 2003-2010
```bash
val result_part1 = spark.sql("SELECT _c1 AS Year, avg((_c29 /_c15)*100) avg_late_aircraft_delay from delay_flights WHERE _c1 >= 2003 AND _c1 <= 2010 GROUP BY Year ORDER BY Year ASC")
```

#### Calculating Year wise security delay from 2003-2010
```bash
val result_part1 = spark.sql("SELECT _c1 AS Year, avg((_c28 /_c15)*100) avg_security_delay from delay_flights WHERE _c1 >= 2003 AND _c1 <= 2010 GROUP BY Year ORDER BY Year ASC")
spark.time(result_part1.show())
```

## Hive app
After accessing the EMR terminal, open the spark application by running the following command.

```bash
hive
```

### Create the table structure

```sql
CREATE TABLE delay_flights (
Id INT, 
Year INT, 
Month INT, 
DayofMonth INT, 
DayOfWeek INT, 
DepTime INT, 
CRSDepTime INT, 
ArrTime INT, 
CRSArrTime INT, 
UniqueCarrier STRING, 
FlightNum INT, 
TailNum STRING, 
ActualElapsedTime INT, 
CRSElapsedTime INT, 
AirTime INT, 
ArrDelay DOUBLE, 
DepDelay DOUBLE, 
Origin STRING,	 
Dest STRING, 
Distance INT, 
TaxiIn INT,	 
TaxiOut INT, 
Cancelled INT, 
CancellationCode STRING, 
Diverted  DOUBLE, 
CarrierDelay INT,  
WeatherDelayINT, 
NASDelayINT, 
SecurityDelay INT, 
LateAircraftDelay INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 's3://flight-delay/MapReduce/';
```

### Load data from S3
```bash
LOAD DATA INPATH 's3://flight-delay/Input/DelayedFlights-att1.csv' OVERWRITE INTO TABLE delay_flights;
```
###Calculations
#### Calculating Year wise carrier delay from 2003-2010
```sql
SELECT Year, avg((CarrierDelay / ArrDelay)*100) avg_carrier_delay from delay_flights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC;
```

#### Calculating Year wise NAS delay from 2003-2010
```sql
SELECT Year, avg((NASDelay /ArrDelay)*100) avg_nas_delay from delay_flights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC;
```

#### Calculating Year wise Weather delay from 2003-2010
```sql
SELECT Year, avg((WeatherDelay /ArrDelay)*100) avg_weather_delay from delay_flights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC;
```

#### Calculating Year wise late aircraft delay from 2003-2010
```sql
SELECT Year, avg((LateAircraftDelay /ArrDelay)*100) avg_late_aircraft_delay from delay_flights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC;
```

#### Calculating Year wise security delay from 2003-2010
```sql
SELECT Year, avg((SecurityDelay /ArrDelay)*100) avg_security_delay from delay_flights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year ORDER BY Year ASC;
```
