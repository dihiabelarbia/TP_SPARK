
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def filter_major_clients(df):
    return df.filter(F.col("age") >= 18)

def join_clients_villes(df_clients, df_villes):
    return df_clients.join(df_villes.dropDuplicates(["zip"]), "zip", "left_outer")

def add_departement_column(zipcode):
    departement = F.when(
        F.substring(F.col("zip"), 1, 2) != "20",
        F.substring(F.col("zip"), 1, 2)
    ).otherwise(
        F.when(F.col("zip") <= 20190, "2A").otherwise("2B") 
    ).alias("departement") 
    return departement

def main():
    spark = SparkSession.builder.appName("clean").getOrCreate()
    clients_df = spark.read.csv("src/resources/exo2/clients_bdd.csv", header=True)
    villes_df = spark.read.csv("src/resources/exo2/city_zipcode.csv", header=True)
    major_clients_df = filter_major_clients(clients_df)
    joined_df = join_clients_villes(major_clients_df, villes_df)
    final_df = joined_df.withColumn("departement" ,add_departement_column(joined_df["zip"]))
    final_df.write.parquet("data/exo2/output", mode="overwrite")
    spark.stop()