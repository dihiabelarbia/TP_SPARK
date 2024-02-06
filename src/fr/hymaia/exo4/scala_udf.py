from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time
from pyspark.sql.functions import lit

from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("ScalaUDF") \
    .config("spark.jars", "src/resources/exo4/udf.jar") \
    .getOrCreate()

def addCategoryName(col):
    
    sc = spark.sparkContext
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))


def main():
    data = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    timeA = time.time()
    df = data.withColumn("category_name", addCategoryName(col("category")))
    timeB = time.time()
    df = df.withColumn("time", lit(timeB - timeA))
    df.show()

    spark.stop()

if __name__ == "__main__":
    main()