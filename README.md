# IRS-Form-990

This project provides examples of how to process IRS-Form-900 data using  **Apache Spark** and **Python**

The dataset is available from [Registry of open data on AWS](https://registry.opendata.aws/irs990/). The dataset contains machine-readable data from certain 
electronic 990 forms filed with the IRS from 2013 to present, via Amazon S3.

Form 990 is the form used by the United States Internal Revenue Service to gather financial information about nonprofit organizations.
Data for each 990 filing is provided in an XML file that contains structured information that represents the main 990 form, 
any filed forms and schedules, and other control information describing how the document was filed.

## Project Structure
A base class IRSSpark.py has been written to set up the configuration and flow of a spark application. Some of the basic parameters of configuration setup here include:

- Number of input partitions i.e number of executors cores desired to be use out of the available cluster.
- Number of output partitions i.e number of result files you want out of the application.
- Output format - default is "parquet" , can be changed toorc, csv or json via command line args.
- Output compression format - default is "gzip", can be changed to snappy or lzo.
- Spark Profiler for logging profiling metrics when the job is finished
- Accumulators to count the nuber of records processed or dropped due to exceptions

Spark and sql context is initialized and every subsequent job can inherit from base class and override the processing function based upon thier customized tasks.

## Processing Methodology
Since the dataset is huge, downloading it to executors disk is not desired hence, the processing is done via buffered io streams. 
The input is a text file having path to all the dataset files. 
The files can be read from local Fs, HDFS or can be directly read from S3 (preferable) since S3 bucket is open for access.
The dataset files are in XML with variable schema for each year and the tag structure is quite complicated to use spark-xml hence the lowest level of API
provided by Spark i.e RDD is being used.
The input file is being divided into partitions (args: no of input paritions).Every partition read the files from S3/HDFS/FS directly as specified via the input paths.
The buffered stream is converted into an Element tree via Python's builtin library and every subsequent job can then work on this element tree in each partition sepaartely.
The end result is finally reduced based on key value, converted into a dataframe according to specified schema and stored as a parquet file in Spark Sql warehouse.

#### To run the Count the Cities job locally
$SPARK_HOME/spark-submit --py-files path_to_IRSSpark.py  path_to_CitiesCountJob.py --num_output_partitions 1 --log_level WARN path_to_input_file citiesnames

#### To run in a cluster
Since the dataset is availabe on AWS S3, the cluster can be set up on AWS EMR and data retrieval will be totally free. Spark installation will be needed.The same
command can be used with modified number of output partitions depending upon the cluster size.


### Sample Output
 A selective sample output for a single file where the task was to calculate the count of cities where IRS-990 forms were filed.


|           City|Count|
|---------------|-----|
|       berkeley|    2|
|fort washington|    2|
|        madison|    3|
|      berkelrey|    1|
|   fayetteville|    3|
|   florham park|    1|
|           cary|    1|
|        raleigh|    2|
|       glenside|    1|
|      shoreline|    2|
|         athens|    1|


## Future Tasks
- Add columnar index for the datasets
- Write shell scripts to download the dataset to HDFS (if required) and containerized the application used Docker
- Automate the pipeline using Airflow
