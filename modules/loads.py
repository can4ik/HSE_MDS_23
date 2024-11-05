from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, SparkSession
from typing import Tuple


def load_to_sdf(spark: SparkSession, path: str) -> DataFrame:
    '''
    Loads a CSV file into a Spark DataFrame.

    Parameters
    ----------
    spark : SparkSession
        The SparkSession object used to load the data.
    path : str
        The path to the CSV file to be loaded.

    Returns
    -------
    DataFrame
        A Spark DataFrame containing the data from the CSV file,
        with headers inferred, UTF-8 encoding, and tab-separated values.
    '''
    return spark.read.csv(path, header=True, encoding='utf-8', sep='\t')


def load_to_tempview(spark: SparkSession, path: str, view_name: str) -> None:
    '''
    Loads a CSV file into a Spark DataFrame and registers it as a temporary view.

    Parameters
    ----------
    spark : SparkSession
        The SparkSession object used to load the data.
    path : str
        The path to the CSV file to be loaded.
    view_name : str
        The name of the temporary view to create.

    Returns
    -------
    None
        The function registers the DataFrame as a temporary view and returns None.
    '''
    clickstream_sdf = spark.read.csv(path, header=True, sep='\t')

    # Register the DataFrame as a temporary view
    return clickstream_sdf.createOrReplaceTempView(view_name)


def _parse_line(line: str) -> Tuple[str, str, str, str, str]:
    '''
    Parses a line of tab-separated data into individual fields.

    Parameters
    ----------
    line : str
        A string representing a line of tab-separated data, expected to contain
        five fields:
        user_id, session_id, event_type, event_page, and timestamp.

    Returns
    -------
    Tuple[str, str, str, str, str]
        A tuple containing the parsed values:
        (user_id, session_id, event_type, event_page, timestamp).
        Returns None if the line does not contain exactly five fields.
    '''
    fields = line.split('\t')
    
    if len(fields) != 5:
        
        return None
    
    user_id, session_id, event_type, event_page, timestamp = fields
    
    return user_id, session_id, event_type, event_page, timestamp


def load_to_rdd(
    sparkContext: SparkContext,
    path: str
    ) -> RDD:
    '''
    Loads data from a text file into an RDD,
    excluding the header and parsing each line.

    Parameters
    ----------
    sparkContext : SparkContext
        The SparkContext object used to load and manipulate the data.
    path : str
        The path to the text file to be loaded.

    Returns
    -------
    RDD
        An RDD containing tuples of parsed data, with each tuple representing
        a record with the fields:
        (user_id, session_id, event_type, event_page, timestamp).
        Malformed lines are filtered out.
    '''
    lines = sparkContext.textFile(path)

    # Extract the header
    header = lines.first()

    # Filter out the header
    lines = lines.filter(lambda x: x != header)

    # Parse records and filter out any malformed lines
    return lines.map(_parse_line).filter(lambda x: x is not None)
