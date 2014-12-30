import glob
import os
import sys

spark_home = os.getenv('SPARK_HOME')
if not spark_home:
    raise ValueError('SPARK_HOME environment variable is not set')
sys.path.insert(0, os.path.join(spark_home, 'python'))
for path in glob.glob(os.path.join(spark_home, 'python/lib/py4j-*.zip')):
    sys.path.insert(0, path)
