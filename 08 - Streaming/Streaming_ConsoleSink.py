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

    # Write the output to console
    consoleQuery = (
                        inputDF
                            .writeStream

                            .queryName("FhvRidesQuery")

                            .format("console")

                            .outputMode("append")

                            .trigger(processingTime='10 seconds')

                            .start()
                   )

    # Wait for termination of query to close the application
    consoleQuery.awaitTermination()









