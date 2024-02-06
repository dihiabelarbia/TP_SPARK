from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

def read_data(spark):
    clients = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True, inferSchema=True)
    villes = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True, inferSchema=True)
    return clients, villes

def filter_majors(clients):
    return clients.filter(clients["age"] >= 18)

def join_with_villes(clients, villes):
    return clients.join(villes, clients["zip"] == villes["zip"]) 


def join_with_villes(clients, villes):
    villes = villes.withColumnRenamed("zip", "city_zip")
    return clients.join(villes, clients["zip"] == villes["city_zip"])


from pyspark.sql.functions import expr

def add_departement(df):
    return df.withColumn("departement", expr("substring(zip, 1, 2)"))

def add_departement(df):
    return df.withColumn("departement", expr("substring(city_zip, 1, 2)"))

def write_to_parquet(df):
    df.write.parquet("data/exo2/output", mode="overwrite")


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
    print( clean_data.select("departement").distinct())
    population_result = calculate_population(clean_data)
    population_result.coalesce(1).write.csv("data/exo2/aggregate", header=True, mode="overwrite")
    print(population_result.dtypes)
    spark.stop()
if __name__ == "__main__":
    main()