import argparse
import json
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile

import boto3
import botocore

import xml.etree.ElementTree as ET
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, StringType, LongType

LOGGING_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'


class IRSSparkJob(object):
    """
    A simple Spark job definition to process IRS data
    """

    name = 'IRSSparkJob'

    output_schema = StructType([
        StructField("key", StringType(), True),
        StructField("val", LongType(), True)
    ])

    # description of input and output shown in --help
    input_descr = "Path to file listing input paths"
    output_descr = "Name of output table (saved in spark.sql.warehouse.dir)"

    args = None
    records_processed = None

    log_level = 'INFO'
    logging.basicConfig(level=log_level, format=LOGGING_FORMAT)

    num_input_partitions = 400
    num_output_partitions = 10

    def parse_arguments(self):
        """ Returns the parsed arguments from the command line """

        description = self.name
        if self.__doc__ is not None:
            description += " - "
            description += self.__doc__
        arg_parser = argparse.ArgumentParser(prog=self.name, description=description,
                                             conflict_handler='resolve')

        arg_parser.add_argument("input", help=self.input_descr)
        arg_parser.add_argument("output", help=self.output_descr)

        arg_parser.add_argument("--num_input_partitions", type=int,
                                default=self.num_input_partitions,
                                help="Number of input splits/partitions, "
                                "number of parallel tasks to process records "
                                "files/records")
        arg_parser.add_argument("--num_output_partitions", type=int,
                                default=self.num_output_partitions,
                                help="Number of output partitions")
        arg_parser.add_argument("--output_format", default="parquet",
                                help="Output format: parquet (default),"
                                " orc, json, csv")
        arg_parser.add_argument("--output_compression", default="gzip",
                                help="Output compression codec: None,"
                                " gzip/zlib (default), snappy, lzo, etc.")
        arg_parser.add_argument("--output_option", action='append', default=[],
                                help="Additional output option pair"
                                " to set (format-specific) output options, e.g.,"
                                " `header=true` to add a header line to CSV files."
                                " Option name and value are split at `=` and"
                                " multiple options can be set by passing"
                                " `--output_option <name>=<value>` multiple times")

        arg_parser.add_argument("--local_temp_dir", default=None,
                                help="Local temporary directory, used to"
                                " buffer content from S3")

        arg_parser.add_argument("--log_level", default=self.log_level,
                                help="Logging level")
        arg_parser.add_argument("--spark-profiler", action='store_true',
                                help="Enable PySpark profiler and log"
                                " profiling metrics if job has finished,"
                                " cf. spark.python.profile")

        self.add_arguments(arg_parser)
        args = arg_parser.parse_args()
        if not self.validate_arguments(args):
            raise Exception("Arguments not valid")
        self.init_logging(args.log_level)

        return args

    def add_arguments(self, parser):
        pass

    def validate_arguments(self, args):
        if "orc" == args.output_format and "gzip" == args.output_compression:
            # gzip for Parquet, zlib for ORC
            args.output_compression = "zlib"
        return True

    def get_output_options(self):
        return {x[0]: x[1] for x in map(lambda x: x.split('=', 1),
                                        self.args.output_option)}

    def init_logging(self, level=None):
        if level is None:
            level = self.log_level
        else:
            self.log_level = level
        logging.basicConfig(level=level, format=LOGGING_FORMAT)

    def init_accumulators(self, sc):
        #actual number of XML element trees processed
        self.records_processed = sc.accumulator(0)
        # number of files successfully downloaded from input path provided
        self.xml_input_processed = sc.accumulator(0)
        #number of files that failed to download
        self.xml_input_failed = sc.accumulator(0)

    def get_logger(self, spark_context=None):
        """Get logger from SparkContext or (if None) from logging module"""
        if spark_context is None:
            return logging.getLogger(self.name)
        return spark_context._jvm.org.apache.log4j.LogManager \
            .getLogger(self.name)

    def run(self):
        self.args = self.parse_arguments()

        conf = SparkConf()

        if self.args.spark_profiler:
            conf = conf.set("spark.python.profile", "true")

        sc = SparkContext(
            appName=self.name,
            conf=conf)
        sqlc = SQLContext(sparkContext=sc)
        streamc = StreamingContext(sc, 1)

        self.init_accumulators(sc)

        self.run_job(sc, sqlc, streamc)

        if self.args.spark_profiler:
            sc.show_profiles()

        sc.stop()

    def log_aggregator(self, sc, agg, descr):
        self.get_logger(sc).info(descr.format(agg.value))

    def log_aggregators(self, sc):
        self.log_aggregator(sc, self.xml_input_processed,
                            'XML input files processed = {}')
        self.log_aggregator(sc, self.xml_input_failed,
                            'XML input files failed = {}')
        self.log_aggregator(sc, self.records_processed,
                            'XML Element tree records processed = {}')

    @staticmethod
    def reduce_by_key_func(a, b):
        return a + b

    @staticmethod
    def get_tags(x):
        tags = [elem.tag for elem in x.iter()]
        for tag in tags:
            for item in x.iter(tag):
                return item.text

    def run_job(self, sc, sqlc, streamc):
        input_data = sc.textFile(self.args.input,
                                 minPartitions=self.args.num_input_partitions)

        output = input_data.mapPartitionsWithIndex(self.process_xmls) \
             .reduceByKey(self.reduce_by_key_func)

        sqlc.createDataFrame(output, schema=self.output_schema).show()
            # .coalesce(self.args.num_output_partitions) \
            # .write \
            # .format(self.args.output_format) \
            # .option("compression", self.args.output_compression) \
            # .options(**self.get_output_options()) \
            # .saveAsTable(self.args.output)

        self.log_aggregators(sc)

    def process_xmls(self, _id, iterator):
        s3pattern = re.compile('^s3://([^/]+)/(.+)')
        base_dir = os.path.abspath(os.path.dirname(__file__))

        # S3 client (not thread-safe, initialize outside parallelized loop)
        no_sign_request = botocore.client.Config(
            signature_version=botocore.UNSIGNED)
        s3client = boto3.client('s3', config=no_sign_request)

        for uri in iterator:
            self.xml_input_processed.add(1)
            if uri.startswith('s3://'):
                self.get_logger().info('Reading from S3 {}'.format(uri))
                s3match = s3pattern.match(uri)
                if s3match is None:
                    self.get_logger().error("Invalid S3 URI: " + uri)
                    continue
                bucketname = s3match.group(1)
                path = s3match.group(2)
                xmltemp = TemporaryFile(mode='w+b',
                                         dir=self.args.local_temp_dir)
                try:
                    s3client.download_fileobj(bucketname, path, xmltemp)
                except botocore.client.ClientError as exception:
                    self.get_logger().error(
                        'Failed to download {}: {}'.format(uri, exception))
                    self.xml_input_failed.add(1)
                    xmltemp.close()
                    continue
                xmltemp.seek(0)
                stream = xmltemp
            # elif uri.startswith('hdfs:/'):
            #     try:
            #         import pydoop.hdfs as hdfs
            #         self.get_logger().error("Reading from HDFS {}".format(uri))
            #         stream = hdfs.open(uri)
            #     except RuntimeError as exception:
            #         self.get_logger().error(
            #             'Failed to open {}: {}'.format(uri, exception))
            #         self.warc_input_failed.add(1)
            #         continue
            else:
                self.get_logger().info('Reading local stream {}'.format(uri))
                if uri.startswith('file:'):
                    uri = uri[5:]
                uri = os.path.join(base_dir, uri)
                try:
                    stream = open(uri, 'rb')
                except IOError as exception:
                    self.get_logger().error(
                        'Failed to open {}: {}'.format(uri, exception))
                    self.xml_input_failed.add(1)
                    continue

           # try:
                # archive_iterator = ArchiveIterator(stream,
                #                                    no_record_parse=no_parse, arc2warc=True)
            element_tree = ET.parse(stream)
            element_tree_tuple = [(elem.tag.strip("{'{http://www.irs.gov/efile}"), elem.text.strip()) for elem in element_tree.iter()]

            # file_rdd = streamc.textFileStream(stream)
            # root_node = file_rdd.map(lambda s: ET.ElementTree(ET.fromstring(s[1])))
            # element_tree = root_node.flatMap(
            #         lambda s: [(elem.tag.strip("{'{http://www.irs.gov/efile}"), elem.text.strip()) for elem in
            #                    s.iter()])
            for res in self.iterate_records(uri, element_tree_tuple):
                yield res
            # except:
            #     self.xml_input_failed.add(1)
            #     self.get_logger().error(
            #         'Invalid XML: {}'.format(uri))
            # finally:
            #     stream.close()

    def process_record(self, record):
        raise NotImplementedError('Processing record needs to be customized')

    def iterate_records(self, _uri, element_tree):
        """Iterate over all XML element trees. This method can be customized
           and allows to access also values from ArchiveIterator, namely
           WARC record offset and length."""
        for res in self.process_record(element_tree):
            yield res
            self.records_processed.add(1)
            # WARC record offset and length should be read after the record
            # has been processed, otherwise the record content is consumed
            # while offset and length are determined:
            #  warc_record_offset = archive_iterator.get_record_offset()
            #  warc_record_length = archive_iterator.get_record_length()

    # @staticmethod
    # def is_wet_text_record(record):
    #     """Return true if WARC record is a WET text/plain record"""
    #     return (record.rec_type == 'conversion' and
    #             record.content_type == 'text/plain')
    #
    # @staticmethod
    # def is_wat_json_record(record):
    #     """Return true if WARC record is a WAT record"""
    #     return (record.rec_type == 'metadata' and
    #             record.content_type == 'application/json')
    #
    # @staticmethod
    # def is_html(record):
    #     """Return true if (detected) MIME type of a record is HTML"""
    #     html_types = ['text/html', 'application/xhtml+xml']
    #     if (('WARC-Identified-Payload-Type' in record.rec_headers) and
    #         (record.rec_headers['WARC-Identified-Payload-Type'] in
    #          html_types)):
    #         return True
    #     content_type = record.http_headers.get_header('content-type', None)
    #     if content_type:
    #         for html_type in html_types:
    #             if html_type in content_type:
    #                 return True
    #     return False
