from pyspark.sql import SparkSession # type: ignore

def run_job():
    spark = SparkSession.builder.appName("ETL Clients").getOrCreate()
    df = spark.read.csv("hdfs://namenode:9000/data/clients.csv",
                        header=True, inferSchema=True)
    avg_age = df.groupBy().avg("age").first()[0]
    print("Âge moyen :", avg_age)
    spark.stop()
    return avg_age

if __name__ == "__main__":
    run_job()


