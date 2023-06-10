from pyspark.sql import *

if __name__ == '__main__':

    spark = (
                SparkSession.builder
                            .appName("SparkApp")
                            .master("local[4]")
                            .getOrCreate()
            )

    numbers = [[1], [2], [3], [4], [5]]

    numbersDF = (
                    spark
                        .createDataFrame
                        (
                            numbers,   # Pass data collection
                            "Id: int"  # Pass schema as string
                        )
                )

    numbersDF.show()

    input("Press Enter to continue...")

