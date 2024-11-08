# # from pyspark.sql import SparkSession

# # # Create a Spark session
# # spark = SparkSession.builder.appName("Advanced Flight Data Analysis").getOrCreate()

# # # Load datasets
# # flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)
# # airports_df = spark.read.csv("airports.csv", header=True, inferSchema=True)
# # carriers_df = spark.read.csv("carriers.csv", header=True, inferSchema=True)

# # # Define output paths
# # output_dir = "output/"
# # task1_output = output_dir + "task1_largest_discrepancy.csv"
# # task2_output = output_dir + "task2_consistent_airlines.csv"
# # task3_output = output_dir + "task3_canceled_routes.csv"
# # task4_output = output_dir + "task4_carrier_performance_time_of_day.csv"

# # # ------------------------
# # # Task 1: Flights with the Largest Discrepancy Between Scheduled and Actual Travel Time
# # # ------------------------
# # def task1_largest_discrepancy(flights_df, carriers_df):
# #     # TODO: Implement the SQL query for Task 1
# #     # Hint: Calculate scheduled vs actual travel time, then find the largest discrepancies using window functions.

# #     # Write the result to a CSV file
# #     # Uncomment the line below after implementing the logic
# #     # largest_discrepancy.write.csv(task1_output, header=True)
# #     print(f"Task 1 output written to {task1_output}")

# # # ------------------------
# # # Task 2: Most Consistently On-Time Airlines Using Standard Deviation
# # # ------------------------
# # def task2_consistent_airlines(flights_df, carriers_df):
# #     # TODO: Implement the SQL query for Task 2
# #     # Hint: Calculate standard deviation of departure delays, filter airlines with more than 100 flights.

# #     # Write the result to a CSV file
# #     # Uncomment the line below after implementing the logic
# #     # consistent_airlines.write.csv(task2_output, header=True)
# #     print(f"Task 2 output written to {task2_output}")

# # # ------------------------
# # # Task 3: Origin-Destination Pairs with the Highest Percentage of Canceled Flights
# # # ------------------------
# # def task3_canceled_routes(flights_df, airports_df):
# #     # TODO: Implement the SQL query for Task 3
# #     # Hint: Calculate cancellation rates for each route, then join with airports to get airport names.

# #     # Write the result to a CSV file
# #     # Uncomment the line below after implementing the logic
# #     # canceled_routes.write.csv(task3_output, header=True)
# #     print(f"Task 3 output written to {task3_output}")

# # # ------------------------
# # # Task 4: Carrier Performance Based on Time of Day
# # # ------------------------
# # def task4_carrier_performance_time_of_day(flights_df, carriers_df):
# #     # TODO: Implement the SQL query for Task 4
# #     # Hint: Create time of day groups and calculate average delay for each carrier within each group.

# #     # Write the result to a CSV file
# #     # Uncomment the line below after implementing the logic
# #     # carrier_performance_time_of_day.write.csv(task4_output, header=True)
# #     print(f"Task 4 output written to {task4_output}")

# # # ------------------------
# # # Call the functions for each task
# # # ------------------------
# # task1_largest_discrepancy(flights_df, carriers_df)
# # task2_consistent_airlines(flights_df, carriers_df)
# # task3_canceled_routes(flights_df, airports_df)
# # task4_carrier_performance_time_of_day(flights_df, carriers_df)

# # # Stop the Spark session
# # spark.stop()
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.window import Window

# # Create a Spark session
# spark = SparkSession.builder.appName("Advanced Flight Data Analysis").getOrCreate()

# # Load datasets
# flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)
# airports_df = spark.read.csv("airports.csv", header=True, inferSchema=True)
# carriers_df = spark.read.csv("carriers.csv", header=True, inferSchema=True)

# # Define output paths
# output_dir = "output/"
# task1_output = output_dir + "task1_largest_discrepancy.csv"
# task2_output = output_dir + "task2_consistent_airlines.csv"
# task3_output = output_dir + "task3_canceled_routes.csv"
# task4_output = output_dir + "task4_carrier_performance_time_of_day.csv"

# # ------------------------
# # Task 1: Flights with the Largest Discrepancy Between Scheduled and Actual Travel Time
# # ------------------------
# def task1_largest_discrepancy(flights_df, carriers_df):
#     flights_df = flights_df.withColumn("scheduled_time", F.col("CRS_ARR_TIME") - F.col("CRS_DEP_TIME"))
#     flights_df = flights_df.withColumn("actual_time", F.col("ARR_TIME") - F.col("DEP_TIME"))
#     flights_df = flights_df.withColumn("discrepancy", F.abs(F.col("actual_time") - F.col("scheduled_time")))

#     window = Window.orderBy(F.desc("discrepancy"))
#     largest_discrepancy = flights_df.withColumn("rank", F.row_number().over(window)) \
#                                     .filter(F.col("rank") <= 10) \
#                                     .join(carriers_df, "CARRIER")

#     largest_discrepancy.select("FL_DATE", "CARRIER", "discrepancy") \
#                        .write.csv(task1_output, header=True)
#     print(f"Task 1 output written to {task1_output}")

# # ------------------------
# # Task 2: Most Consistently On-Time Airlines Using Standard Deviation
# # ------------------------
# def task2_consistent_airlines(flights_df, carriers_df):
#     consistent_airlines = flights_df.groupBy("CARRIER") \
#                                     .agg(F.count("*").alias("flight_count"),
#                                          F.stddev("DEP_DELAY").alias("stddev_delay")) \
#                                     .filter(F.col("flight_count") > 100) \
#                                     .orderBy(F.asc("stddev_delay")) \
#                                     .join(carriers_df, "CARRIER")

#     consistent_airlines.select("CARRIER", "flight_count", "stddev_delay") \
#                        .write.csv(task2_output, header=True)
#     print(f"Task 2 output written to {task2_output}")

# # ------------------------
# # Task 3: Origin-Destination Pairs with the Highest Percentage of Canceled Flights
# # ------------------------
# def task3_canceled_routes(flights_df, airports_df):
#     canceled_routes = flights_df.groupBy("ORIGIN", "DEST") \
#                                 .agg(F.sum(F.when(F.col("CANCELLED") == 1, 1).otherwise(0)).alias("canceled"),
#                                      F.count("*").alias("total")) \
#                                 .withColumn("cancellation_rate", F.col("canceled") / F.col("total")) \
#                                 .orderBy(F.desc("cancellation_rate")) \
#                                 .join(airports_df.alias("orig"), flights_df.ORIGIN == airports_df.IATA, "inner") \
#                                 .join(airports_df.alias("dest"), flights_df.DEST == airports_df.IATA, "inner") \
#                                 .select(F.col("orig.Name").alias("origin_name"),
#                                         F.col("dest.Name").alias("dest_name"),
#                                         "cancellation_rate")

#     canceled_routes.write.csv(task3_output, header=True)
#     print(f"Task 3 output written to {task3_output}")

# # ------------------------
# # Task 4: Carrier Performance Based on Time of Day
# # ------------------------
# def task4_carrier_performance_time_of_day(flights_df, carriers_df):
#     flights_df = flights_df.withColumn("time_of_day",
#                                        F.when((F.col("CRS_DEP_TIME") >= 500) & (F.col("CRS_DEP_TIME") < 1200), "morning")
#                                        .when((F.col("CRS_DEP_TIME") >= 1200) & (F.col("CRS_DEP_TIME") < 1700), "afternoon")
#                                        .when((F.col("CRS_DEP_TIME") >= 1700) & (F.col("CRS_DEP_TIME") < 2100), "evening")
#                                        .otherwise("night"))
    
#     carrier_performance = flights_df.groupBy("CARRIER", "time_of_day") \
#                                     .agg(F.avg("DEP_DELAY").alias("avg_delay")) \
#                                     .orderBy("CARRIER", "time_of_day") \
#                                     .join(carriers_df, "CARRIER")

#     carrier_performance.select("CARRIER", "time_of_day", "avg_delay") \
#                        .write.csv(task4_output, header=True)
#     print(f"Task 4 output written to {task4_output}")

# # ------------------------
# # Call the functions for each task
# # ------------------------
# task1_largest_discrepancy(flights_df, carriers_df)
# task2_consistent_airlines(flights_df, carriers_df)
# task3_canceled_routes(flights_df, airports_df)
# task4_carrier_performance_time_of_day(flights_df, carriers_df)

# # Stop the Spark session
# spark.stop()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark session
spark = SparkSession.builder.appName("Advanced Flight Data Analysis").getOrCreate()

# Load datasets
flights_df = spark.read.csv("flights.csv", header=True, inferSchema=True)
airports_df = spark.read.csv("airports.csv", header=True, inferSchema=True)
carriers_df = spark.read.csv("carriers.csv", header=True, inferSchema=True)

# Define output paths
output_dir = "output/"
task1_output = output_dir + "task1_largest_discrepancy.csv"
task2_output = output_dir + "task2_consistent_airlines.csv"
task3_output = output_dir + "task3_canceled_routes.csv"
task4_output = output_dir + "task4_carrier_performance_time_of_day.csv"

# ------------------------
# Task 1: Flights with the Largest Discrepancy Between Scheduled and Actual Travel Time
# ------------------------
def task1_largest_discrepancy(flights_df, carriers_df):
    flights_df = flights_df \
        .withColumn("scheduled_time", F.unix_timestamp("ScheduledArrival") - F.unix_timestamp("ScheduledDeparture")) \
        .withColumn("actual_time", F.unix_timestamp("ActualArrival") - F.unix_timestamp("ActualDeparture")) \
        .withColumn("discrepancy", F.abs(F.col("actual_time") - F.col("scheduled_time")))

    window = Window.orderBy(F.desc("discrepancy"))
    largest_discrepancy = flights_df.withColumn("rank", F.row_number().over(window)) \
                                    .filter(F.col("rank") <= 10) \
                                    .join(carriers_df, flights_df.CarrierCode == carriers_df.CarrierCode) \
                                    .select("FlightNum", "CarrierName", "discrepancy","rank")

    largest_discrepancy.write.csv(task1_output, header=True,mode="overwrite")
    print(f"Task 1 output written to {task1_output}")

# ------------------------
# Task 2: Most Consistently On-Time Airlines Using Standard Deviation
# ------------------------
def task2_consistent_airlines(flights_df, carriers_df):
    consistent_airlines = flights_df \
        .withColumn("departure_delay", F.unix_timestamp("ActualDeparture") - F.unix_timestamp("ScheduledDeparture")) \
        .groupBy("CarrierCode") \
        .agg(F.count("*").alias("flight_count"),
             F.stddev("departure_delay").alias("stddev_delay")) \
        .filter(F.col("flight_count") > 100) \
        .orderBy(F.asc("stddev_delay")) \
        .join(carriers_df, "CarrierCode") \
        .select("CarrierCode", "CarrierName", "stddev_delay")

    consistent_airlines.write.csv(task2_output, header=True, mode="overwrite")
    print(f"Task 2 output written to {task2_output}")

# ------------------------
# Task 3: Origin-Destination Pairs with the Highest Percentage of Canceled Flights
# ------------------------
# def task3_canceled_routes(flights_df, airports_df):
#     # Calculate cancellation rate for each route
#     canceled_routes = flights_df \
#         .withColumn("canceled", F.when(F.col("ActualDeparture").isNull(), 1).otherwise(0)) \
#         .groupBy("Origin", "Destination") \
#         .agg(F.sum("canceled").alias("canceled_count"),
#              F.count("*").alias("total_flights")) \
#         .withColumn("cancellation_rate", F.col("canceled_count") / F.col("total_flights")) \
#         .orderBy(F.desc("cancellation_rate"))

#     # Join with airports to get full airport names, using aliases to avoid ambiguity
#     origin_airports = airports_df.alias("orig")
#     destination_airports = airports_df.alias("dest")

#     canceled_routes = canceled_routes \
#         .join(origin_airports, canceled_routes.Origin == origin_airports.AirportCode, "inner") \
#         .join(destination_airports, canceled_routes.Destination == destination_airports.AirportCode, "inner") \
#         .select(F.col("orig.AirportName").alias("origin_airport"),
#                 F.col("dest.AirportName").alias("destination_airport"),
#                 "cancellation_rate")

#     # Write the result to a CSV file
#     canceled_routes.write.csv(task3_output, header=True, mode="overwrite")
#     print(f"Task 3 output written to {task3_output}")

def task3_canceled_routes(flights_df, airports_df):
    # Register DataFrames as SQL temporary views to use Spark SQL
    flights_df.createOrReplaceTempView("flights")
    airports_df.createOrReplaceTempView("airports")

    # SQL query to calculate cancellation rate and rank routes
    canceled_routes_query = """
    SELECT 
        f.Origin,
        f.Destination,
        COUNT(CASE WHEN f.ActualDeparture IS NULL THEN 1 END) AS canceled_count,
        COUNT(*) AS total_flights,
        (COUNT(CASE WHEN f.ActualDeparture IS NULL THEN 1 END) / COUNT(*)) AS cancellation_rate
    FROM 
        flights f
    GROUP BY 
        f.Origin, f.Destination
    ORDER BY 
        cancellation_rate DESC
    """

    # Execute the SQL query
    canceled_routes_df = spark.sql(canceled_routes_query)

    # Register the result as a temporary view to join with airports
    canceled_routes_df.createOrReplaceTempView("canceled_routes")

    # SQL query to join with airports for full names of origin and destination
    full_canceled_routes_query = """
    SELECT 
        ROW_NUMBER() OVER (ORDER BY cr.cancellation_rate DESC) AS rank,
        orig.AirportName AS origin_airport,
        dest.AirportName AS destination_airport,
        cr.cancellation_rate
    FROM 
        canceled_routes cr
    JOIN 
        airports orig ON cr.Origin = orig.AirportCode
    JOIN 
        airports dest ON cr.Destination = dest.AirportCode
    ORDER BY 
        cr.cancellation_rate DESC
    """

    # Execute the SQL query to get the full names and cancellation rates
    full_canceled_routes_df = spark.sql(full_canceled_routes_query)

    # Write the result to a CSV file
    full_canceled_routes_df.write.csv(task3_output, header=True, mode="overwrite")
    print(f"Task 3 output written to {task3_output}")

# ------------------------
# Task 4: Carrier Performance Based on Time of Day
# ------------------------
def task4_carrier_performance_time_of_day(flights_df, carriers_df):
    # Register DataFrames as SQL temporary views to use Spark SQL
    flights_df.createOrReplaceTempView("flights")
    carriers_df.createOrReplaceTempView("carriers")

    # Create a temporary view with time of day categories
    time_of_day_query = """
    SELECT 
        CarrierCode,
        CASE 
            WHEN HOUR(ScheduledDeparture) BETWEEN 06 AND 12 THEN 'morning'
            WHEN HOUR(ScheduledDeparture) BETWEEN 12 AND 18 THEN 'afternoon'
            WHEN HOUR(ScheduledDeparture) BETWEEN 18 AND 06 THEN 'evening'
            ELSE 'night'
        END AS time_of_day,
        (UNIX_TIMESTAMP(ActualDeparture) - UNIX_TIMESTAMP(ScheduledDeparture)) AS departure_delay
    FROM 
        flights
    """
    
    # Execute the SQL query to create a DataFrame with time_of_day and departure_delay
    time_of_day_df = spark.sql(time_of_day_query)
    time_of_day_df.createOrReplaceTempView("time_of_day_delays")

    # Calculate average delay, rank carriers within each time period, and join with carriers for full names
    carrier_performance_query = """
    SELECT 
        c.CarrierName,
        t.time_of_day,
        AVG(t.departure_delay) AS avg_delay,
        RANK() OVER (PARTITION BY t.time_of_day ORDER BY AVG(t.departure_delay) ASC) AS rank
    FROM 
        time_of_day_delays t
    JOIN 
        carriers c ON t.CarrierCode = c.CarrierCode
    GROUP BY 
        c.CarrierName, t.time_of_day
    ORDER BY 
        t.time_of_day, rank
    """

    # Execute the final SQL query to get the average delay, ranking, and time of day
    carrier_performance_df = spark.sql(carrier_performance_query)

    # Write the result to a CSV file
    carrier_performance_df.write.csv(task4_output, header=True, mode="overwrite")
    print(f"Task 4 output written to {task4_output}")

# ------------------------
# Call the functions for each task
# ------------------------
task1_largest_discrepancy(flights_df, carriers_df)
task2_consistent_airlines(flights_df, carriers_df)
task3_canceled_routes(flights_df, airports_df)
task4_carrier_performance_time_of_day(flights_df, carriers_df)

# Stop the Spark session
spark.stop()
