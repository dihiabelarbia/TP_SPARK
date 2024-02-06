from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def word_count(spark):
    # Lire le fichier CSV
    input_file = "./spark-handson/src/resources/exo1/data.csv"
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Appliquer la fonction WordCount sur le DataFrame
    word_counts = df.select("word").groupBy("word").count()

    # Écrire le résultat au format Parquet avec partitionnement par "count"
    output_path = "data/exo1/output"
    word_counts.write.partitionBy("count").parquet(output_path, mode="overwrite")

if __name__ == "__main__":
    # Initialiser la session Spark
    spark = SparkSession.builder.appName("wordcount").getOrCreate()

    try:
        word_count(spark)
    finally:
        # Arrêter la session Spark lorsque vous avez terminé
        spark.stop()

