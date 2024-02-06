from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import SparkSession
from src.fr.hymaia.exo2.spark_aggregate_job import calculate_population
from src.fr.hymaia.exo2.spark_clean_job import filter_major_clients


class TestCalculatePopulation(unittest.TestCase):

    def test_calculate_population(self):
        # Given
        df = spark.createDataFrame(
            [
                (75650, "dihia", 30, "mirabeau city", "75"),
                (20167, "paul", 25, "argenteuil", "2A"),
                (20235, "Pauley", 47, "CANAVAGGIA", "2B"),
                (10304, "Jean", 40, "Ajaccio", "10"),
                (75404, "berber", 70, "gottam city", "75")
            ],
            ["zip", "name", "age", "city", "departement"]
        )

        # When
        result = calculate_population(df)

        # Then
        actual_list = sorted([(row["departement"], row["nb_people"]) for row in result.collect()], key=lambda x: x[0])
        expected = sorted([('75', 2), ('10', 1), ('2A', 1), ('2B', 1)], key=lambda x: x[0])

        self.assertEqual(actual_list, expected)


    def test_filter_major_clients(self):
        input_data = spark.createDataFrame([(1, "John", 17), (2, "Jane", 25)], ["id", "name", "age"])
        expected_data = spark.createDataFrame([(2, "Jane", 25)], ["id", "name", "age"])

        actual_data = filter_major_clients(input_data)

        self.assertCountEqual(actual_data.collect(), expected_data.collect())

if __name__ == '__main__':
    unittest.main()
