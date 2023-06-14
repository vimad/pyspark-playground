from pyspark.sql import *
from pyspark.sql.functions import *

if __name__ == '__main__':

    # Create Spark Session
    spark = (
                SparkSession
                    .builder
                    .appName("SparkStructuredStreamingApp")
                    .master("local[4]")
                    .config("spark.dynamicAllocation.enabled", "false")
                    .getOrCreate()
            )

    # Set log level
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Define schema for FHV Rides
    rideSchema = (
                    StructType()
                        .add("Id", "integer")
                        .add("VendorId", "integer")
                        .add("PickupTime", "timestamp")
                        .add("DropTime", "timestamp")
                        .add("PickupLocationId", "integer")
                        .add("DropLocationId", "integer")
                        .add("CabLicense", "string")
                        .add("DriverLicense", "string")
                        .add("PassengerCount", "integer")
                        .add("RateCodeId", "integer")
                )

    # Create a Streaming DataFrame
    inputDF = (
                    spark
                        .readStream

                        .schema(rideSchema)

                        .option("maxFilesPerTrigger", 1)

                        .option("multiline", "true")
                        .json("C:\SparkCourse\DataFiles\Raw\Streaming")
              )

    # Check if DataFrame is streaming or not
    print("Is inputDF Streaming DataFrame = " + str( inputDF.isStreaming) )

    # Define an expression for TripType column
    tripTypeColumn = (
                        when(
                            col("RatecodeID") == 6,
                            "SharedTrip"
                        )
                        .otherwise("SoloTrip")
                     )

    # Transform the data
    transformedDF = (
                      inputDF

                        # Select limited columns
                        .select(
                                    "VendorId", "PickupTime", "DropTime",
                                    "PickupLocationId", "DropLocationId",
                                    "PassengerCount", "RateCodeId"
                               )

                        # Filter data
                        .where("PassengerCount > 0")

                        # Create derived column - TripTimeInMinutes
                        .withColumn("TripTimeInMinutes",
                                        round(
                                                (unix_timestamp(col("DropTime"))
                                                    - unix_timestamp(col("PickupTime")))
                                                 / 60
                                             )
                                    )

                        # Create derived column - TripType
                        .withColumn("TripType", tripTypeColumn)

                        # Drop column
                        .drop("RateCodeId")
                    )

    # Write the output to file sink
    fileQuery = (
                    transformedDF
                    .writeStream
                    .queryName("FhvRidesFileQuery")

                    .format("csv")
                    .option("path", "C:\SparkCourse\DataFiles\Output\Streaming")

                    .option("checkpointLocation", "C:\SparkCourse\DataFiles\Output\Streaming\Checkpoints")

                    .outputMode("append")

                    .trigger(processingTime='5 seconds')

                    .start()
                )

    # Using Spark SQL and running 2 streaming jobs together
    transformedDF.createOrReplaceTempView("TaxiData")

    # Write SQL query
    sqlDF = spark.sql("""
                            SELECT PickupLocationId,
                                   COUNT(*) AS Count
                            FROM TaxiData
                            GROUP BY PickupLocationId
                      """)

    # Write the output to console sink
    sqlQuery = (
                    sqlDF
                    .writeStream
                    .queryName("FhvRidesSqlQuery")
                    .format("console")
                    .outputMode("complete")
                    .trigger(processingTime='10 seconds')
                    .start()
               )

    sqlQuery.awaitTermination()









