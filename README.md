edx-idea
========

Getting Started
---------------

1. Download [Apache Spark](https://spark.apache.org/downloads.html) version 1.2.0 built for Hadoop 2.4
2. Extract the archive somewhere and export a SPARK_HOME environment variable to point to that location
3. Copy the example/hive-site.xml to $SPARK_HOME/conf/hive-site.xml
4. Modify $SPARK_HOME/conf/hive-site.xml include your AWS credentials (or remove the properties)
5. Install this project using `pip install -e ".[ipython,examples]"`
6. Run the example: `cd example; idea sample_workflow.yml ../ipython/gibberish.txt`

Running Interactively
---------------------

```bash
cd ipython
ipython notebook --profile idea
```