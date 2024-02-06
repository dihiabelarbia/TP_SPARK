from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.functions import expr


def calculate_population(df):
    result = (
        df.groupBy("departement")
        .agg({"name": "count"})
        .withColumnRenamed("count(name)", "nb_people")
        .orderBy(desc("nb_people"))
    )
    return result

def main():
    spark = SparkSession.builder.appName("aggregate").getOrCreate()
    clean_data = spark.read.parquet("data/exo2/output")
    population_result = calculate_population(clean_data)
    population_result.coalesce(1).write.csv("data/exo2/aggregate", header=True, mode="overwrite")

    spark.stop()
if __name__ == "__main__":
    main()