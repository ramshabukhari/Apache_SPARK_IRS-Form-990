from functools import reduce

from pyspark.sql import SparkSession,SQLContext
#from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.types import IntegerType
import xml.etree.ElementTree as ET
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark import RDD as rdd
from IRSSpark import IRSSparkJob


class CitiesCountJob(IRSSparkJob):
    """ Count the number of tax files from each city in States"""

    name = "CitiesCount"

    output_schema = StructType([StructField("City", StringType(), True),
                                StructField("Count", LongType(), True)])


# spark = SparkSession.builder.master("local").config("spark.sql.autoBroadcastJoinThreshold", -1).config("spark.executor.memory", "3g").appName("reading_data").getOrCreate()
#
# sqlc = SQLContext(spark.sparkContext)
# file_rdd = spark.sparkContext.wholeTextFiles("/home/ramshabukhari/big_data_projects/irs-form-990/2020/*.xml")
# root = file_rdd.map(lambda s:  ET.ElementTree(ET.fromstring(s[1])))
#
# final= root.flatMap(lambda s: [(elem.tag.strip("{'{http://www.irs.gov/efile}") , elem.text.strip()) for elem in s.iter()])
    @staticmethod
    def reduce_by_key_func(a, b):
        return a+b


    def process_record(self, record):
        filtered = list(filter(lambda x : (x[0]=='CityNm'), record))

        check = list(map(lambda x : (x[1].lower() , 1), filtered))

     #  output = record.groupByKey().filter(lambda x : x[0]=='CityNm').mapPartitions(get_city_names_tuples).flatMap(lambda x:x).reduceByKey(reduce_by_func)
        return check

if __name__ == '__main__':
    job = CitiesCountJob()
    job.run()