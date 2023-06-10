from pyspark.sql import SparkSession

"""
A. In Command Prompt 1 - Spark Master

A.1. Create Spark master
	=> spark-class org.apache.spark.deploy.master.Master --host <machine name or localhost>

	- Example => spark-class org.apache.spark.deploy.master.Master --host CrystalTalks-PC

A.2. Copy Master URL and Spark UI URL
	- Typically master URL is at port 7077
	- Typically Spark UI is at port 4040
	- If any of these ports are in use, a different port might be used

----------------------------------------------------------------------------------------------------------

B. In Command Prompt 2 - Worker Node 1

B.1. Create worker node 1
	=> spark-class org.apache.spark.deploy.worker.Worker <master URL> --host <machine name or localhost>

	- Example => spark-class org.apache.spark.deploy.worker.Worker spark://CrystalTalks-PC:7077 --host CrystalTalks-PC

----------------------------------------------------------------------------------------------------------

C. In Command Prompt 3 - Worker Node 2

C.1. Create worker node 2
	=> spark-class org.apache.spark.deploy.worker.Worker <master URL> --host <machine name or localhost>

	- Example => spark-class org.apache.spark.deploy.worker.Worker spark://CrystalTalks-PC:7077 --host CrystalTalks-PC

----------------------------------------------------------------------------------------------------------

D. Testing multi-node cluster setup with Spark Submit command

D.1. Run spark submit command
	=> spark-submit --master <master URL> --name SparkSubmitApp --total-executor-cores 4 --executor-memory 2g --executor-cores 2 "<file path>"

	- Example => spark-submit --master spark://CrystalTalks-PC:7077 --name SparkSubmitApp --total-executor-cores 4 --executor-memory 2g --executor-cores 2 "\Spark3Fundamentals\SparkSubmitTest.py
"""

if __name__ == '__main__':
    spark = (
        SparkSession.builder
        .appName("JupyterApp")

        .master("spark://localhost:7077")

        .config("spark.dynamicAllocation.enabled", "false")

        .config("spark.cores.max", "4")

        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")

        .getOrCreate()
    )

    numbers = [[1], [2], [3], [4], [5]]

    numbersDF = (
        spark
        .createDataFrame
            (
            numbers,  # Pass data collection
            "Id: int"  # Pass schema as string
        )
    )

    numbersDF.show()

    input("Press Enter to continue...")
