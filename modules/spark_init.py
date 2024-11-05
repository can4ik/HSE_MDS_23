import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from typing import Tuple


def create_spark(appname: str) -> Tuple[SparkSession, SparkContext]:
    '''
    Creates a SparkSession and SparkContext for the given application name.

    Parameters
    ----------
    appname : str
        The name of the Spark application.

    Returns
    -------
    tuple(SparkSession, SparkContext)
        A tuple containing the SparkSession and SparkContext objects
        for the given application.
    '''
    sparkContext = SparkContext(appName=appname)
    sparkContext.addPyFile('file:///home/jovyan/work/modules.zip')
    
    spark = SparkSession(sparkContext)
    
    return spark, sparkContext