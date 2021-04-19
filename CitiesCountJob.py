from pyspark.sql.types import StructType, StructField, StringType, LongType

from IRSSpark import IRSSparkJob


class CitiesCountJob(IRSSparkJob):
    """ Count the number of tax files from each city in States"""

    name = "CitiesCount"

    output_schema = StructType([StructField("City", StringType(), True),
                                StructField("Count", LongType(), True)])

    @staticmethod
    def reduce_by_key_func(a, b):
        return a + b

    def process_record(self, record):
        city_name_col = list(filter(lambda x: (x[0] == 'CityNm'), record))

        cities_with_count = list(map(lambda x: (x[1].lower(), 1), city_name_col))

        return cities_with_count


if __name__ == '__main__':
    job = CitiesCountJob()
    job.run()
