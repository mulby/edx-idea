try:
    from pyspark import SparkContext
    from pyspark.sql import HiveContext
except ImportError:
    pass

from edx.idea.common.singleton import Singleton
from edx.idea.config import Configuration


class Context(object):
    __metaclass__ = Singleton

    def __init__(self):
        config = Configuration()
        self.spark = SparkContext(appName=config.get_nested('spark', 'application_name', default='idea'))
        self.hive = HiveContext(self.spark)

    def stop(self):
        self.spark.stop()
