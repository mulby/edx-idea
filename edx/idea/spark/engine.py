import logging
import subprocess
import sys

try:
    from pyspark.sql import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, BinaryType, BooleanType, DateType, TimestampType, DecimalType, ByteType, ShortType, LongType

    FROM_RDD_TYPE = {
        StringType: 'string',
        IntegerType: 'integer',
        FloatType: 'float',
        DoubleType: 'double',
        BinaryType: 'binary',
        BooleanType: 'boolean',
        DateType: 'date',
        TimestampType: 'timestamp',
        DecimalType: 'decimal',
        ByteType: 'tinyint',
        ShortType: 'smallint',
        LongType: 'bigint',
    }
    TO_RDD_TYPE = dict((v, k) for k, v in FROM_RDD_TYPE.iteritems())
except ImportError:
    pass

from edx.idea.common.identifier import generate_uuid
from edx.idea.config import Configuration
from edx.idea.data_frame import DataFrame
from edx.idea.schema import Field, Schema
from edx.idea.spark.context import Context


log = logging.getLogger(__name__)
TO_HIVE_TYPE = {
    'string': 'STRING',
    'integer': 'INT',
    'float': 'FLOAT',
    'double': 'DOUBLE',
    'binary': 'BINARY',
    'boolean': 'BOOLEAN',
    'date': 'DATE',
    'timestamp': 'TIMESTAMP',
    'decimal': 'DECIMAL',
    'tinyint': 'TINYINT',
    'smallint': 'SMALLINT',
    'bigint': 'BIGINT'
}


def reducer_driver(reduce_function):
    def reducer(t):
        key, values = t
        for item in reduce_function(key, values):
            yield item
    return reducer


def convert_namedtuple(record):
    if hasattr(record, '_fields'):
        return tuple(zip(record._fields, tuple(record)))
    else:
        return record


class SparkEngine(object):

    @property
    def context(self):
        if not hasattr(self, '_context'):
            self._context = Context()
        return self._context

    def map(self, data_frame, map_function):
        return self.from_rdd(data_frame.rdd.flatMap(map_function))

    def map_reduce(self, data_frame, map_function, reduce_function):
        return self.from_rdd(data_frame.rdd.flatMap(map_function).groupByKey().flatMap(reducer_driver(reduce_function)))

    def filter(self, data_frame, filter_function):
        return self.from_rdd(data_frame.rdd.filter(filter_function))

    def take(self, data_frame, n_records):
        return data_frame.rdd.take(n_records)

    def collect(self, data_frame):
        return data_frame.rdd.collect()

    def each(self, data_frame, each_function):
        data_frame.rdd.foreach(each_function)

    def count(self, data_frame):
        return data_frame.rdd.count()

    def cache(self, data_frame):
        table_name = getattr(data_frame, 'table_name', None)
        if table_name:
            self.context.hive.cacheTable(table_name)
        else:
            data_frame.rdd.cache()

    def to_table(self, data_frame, table_name=None, schema=None, primary_key=None):
        if not table_name:
            table_name = getattr(data_frame, 'table_name', None)
            if not table_name:
                raise ValueError('This DataFrame does not have a valid table name.')

        schema = schema or getattr(data_frame, 'schema', None)
        if not schema:
            try:
                rdd_schema = data_frame.rdd.schema()
            except AttributeError:
                try:
                    schema_rdd = self.context.hive.inferSchema(data_frame.rdd.map(convert_namedtuple))
                except (TypeError, ValueError):
                    log.exception('Unable to infer schema for DataFrame.')
                    raise ValueError('This DataFrame does not have a valid schema.')

                rdd_schema = schema_rdd.schema()
            else:
                schema_rdd = data_frame.rdd

            idea_schema_fields = []
            for rdd_field in rdd_schema.fields:
                data_type = FROM_RDD_TYPE[type(rdd_field.dataType)]
                idea_schema_fields.append(Field(rdd_field.name, data_type))
            schema = Schema(fields=idea_schema_fields, primary_key=primary_key)
        else:
            try:
                rdd_schema = data_frame.rdd.schema()
            except AttributeError:
                struct_fields = []
                for _, field in schema.fields.iteritems():
                    data_type = TO_RDD_TYPE[field.data_type]
                    struct_fields.append(StructField(field.name, data_type(), True))
                rdd_schema = StructType(struct_fields)
                schema_rdd = self.context.hive.applySchema(data_frame.rdd, rdd_schema)
            else:
                schema_rdd = data_frame.rdd

        log.info('Saving table %s.', table_name)
        log.debug('Table Schema = %s.', str(schema))
        log.debug('RDD Schema = %s.', schema_rdd.schemaString())

        temp_table_name = 'transfer_' + generate_uuid()
        schema_rdd.registerTempTable(temp_table_name)
        log.debug('Registered temporary table %s.', temp_table_name)

        create_table_statement = """
            CREATE TABLE IF NOT EXISTS {table_name} (
                {column_defs}
            )
        """.format(
            table_name=table_name,
            column_defs=','.join([f.name + ' ' + TO_HIVE_TYPE[f.data_type] for f in schema.fields_without_key()]),
        )
        if schema.primary_key:
            create_table_statement += " PARTITIONED BY ({key_name} {key_type})".format(
                key_name=schema.primary_key.name,
                key_type=TO_HIVE_TYPE[schema.primary_key.data_type]
            )
        self.context.hive.sql(create_table_statement)

        columns = [f.name for f in schema.fields_without_key()]
        partition = ''
        if schema.primary_key:
            # Note that the partition columns must be the last columns in the select statement
            partition = ' PARTITION({primary_key})'.format(primary_key=schema.primary_key.name)
            columns += [schema.primary_key.name]

        self.context.hive.sql('INSERT OVERWRITE TABLE {table_name}{partition} SELECT {columns} FROM {temp_table_name}'
            .format(
                table_name=table_name,
                partition=partition,
                temp_table_name=temp_table_name,
                columns=','.join(columns),
            )
        )

        res_df = self.from_rdd(schema_rdd)
        res_df.table_name = table_name
        res_df.schema = schema
        return res_df

    def from_sql_query(self, query):
        rdd = self.context.hive.sql(query)
        return self.from_rdd(rdd)

    def from_table(self, table_name):
        rdd = self.context.hive.table(table_name)
        df = self.from_rdd(rdd)
        df.table_name = table_name
        return df

    def from_url(self, url):
        return self.from_rdd(self.context.spark.textFile(url))

    def from_list(self, data):
        return self.from_rdd(self.context.spark.parallelize(data))

    def from_rdd(self, rdd):
        data_frame = DataFrame()
        data_frame.rdd = rdd
        return data_frame

    def run(self, step):
        config = Configuration()
        home = config.get_env('spark', 'home', env_var='SPARK_HOME')
        master = config.get_env('spark', 'master', env_var='SPARK_MASTER', default='local[*]')
        python_exe = config.get_env('spark', 'python', env_var='SPARK_PYTHON', default=sys.executable)

        environment = {
            'SPARK_HOME': home,
            'MASTER': master,
            'PYSPARK_PYTHON': python_exe,
        }

        cmd = [
            '{home}/bin/spark-submit'.format(home=home),
            '--verbose',
            step.path
        ] + step.args

        log.debug('Running spark-submit. cmd=%s, env=%s', str(cmd), str(environment))

        subprocess.check_call(cmd, env=environment)
