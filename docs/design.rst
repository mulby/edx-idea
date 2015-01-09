Goal
====

Run large and small scale batch analysis to develop reusable data models that can be used to derive other models.

Opinions
========

* SQL can model many useful transformations with minimal boilerplate code and data type handling code.
* Map reduce can model many other useful parallelizable transformations.
* Transformations should be idempotent.
* Most data can be partitioned using a primary key.
* Recomputation is performed relatively infrequently.
* The topology of the task dependency graph changes infrequently.
* A simple development workflow is critical to building a community of contributors.
* A simple operational deployment and support strategy will help increase adoption.
* Data models should be self-documenting.
* Scheduling execution can be handled by something else (cron, jenkins etc).

Not addressed (yet)
===================

* Streaming and near real time computation.

Example Extension
=================

The sample extension is assumed to be a python package that can be installed. It must define an entry point for each task that can be executed.

workflow.yml
------------

.. code:: YAML

    name: Distinct Countries
    phases:
      - steps:
        - id: edx.task.word_count
          args:
            - "{{ argv[0] }}"

setup.py
--------

.. code:: python

    setup(
        # ...
        entry_points={
            'edx.idea.task': [
                'edx.task.word_count = edx.analytics.workflows.word_count:WordCountTask',
            ]
        }
    )

edx/analytics/workflows/word_count.py
-------------------------------------

.. code:: python

    from edx.idea.data_frame import DataFrame


    class WordCountTask(object):

        def run(self, *args):
            df = DataFrame.from_url(args[0])
            counted_words = df.map_reduce(WordCountTask.mapper, WordCountTask.reducer)
            print counted_words.collect()

        @staticmethod
        def mapper(line):
            for word in line.split(' '):
                yield (word.rstrip(".,\r\n").lower(), 1)

        @staticmethod
        def reducer(word, counts):
            yield (word, sum(counts))


Execution
---------

.. code:: bash

    idea workflow.yml some/text/file.txt


Example Interactive Session
===========================


.. code:: python

    from edx.idea.data_frame import DataFrame
    from edx.idea.sql import sql_query
    from collections import namedtuple

Loading data to work with
-------------------------

.. code:: python

    df = DataFrame.from_url('gibberish.txt')

Note that the following command only reads part of the file and returns
the first 5 lines. It need not read the entire file.

.. code:: python

    df.take(5)



.. parsed-literal::

    [u'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
     u'Integer euismod lacus nec mi dignissim porta.',
     u'Aenean vel libero ac nulla sodales lacinia in vitae ex.',
     u'Fusce vitae orci id erat pretium aliquet et vel augue.',
     u'Aenean cursus nisl vitae facilisis vehicula.']



Word Count (obligatory)
-----------------------

Below is the classic word count problem implemented using this API. Note
that the ``wc_mapper`` and ``wc_reducer`` closures are actually
serialized and shipped to slave nodes to execute over various partitions
of the data.

.. code:: python

    def wc_mapper(line):
        for word in line.split(' '):
            yield (word.rstrip(".,\r\n").lower(), 1)
    
    def wc_reducer(word, counts):
        yield (word, sum(counts))
    
    counted_words = df.map_reduce(wc_mapper, wc_reducer)

After the previous code was executed, no computation has actually been
done. Once I actually request the results of the compuation, it is the
promise is resolved, data is computed and then returned to the driver.

.. code:: python

    dict(counted_words.collect()[:10])



.. parsed-literal::

    {u'ac': 18,
     u'auctor': 14,
     u'felis': 14,
     u'justo': 8,
     u'quam': 6,
     u'sagittis': 6,
     u'semper': 8,
     u'suscipit': 9,
     u'urna': 17,
     u'varius': 4}



SQL
---

.. code:: python

    TextLine = namedtuple('TextLine', ['line'])

In order to know what to call columns etc, the data must be structured
somewhat. We use a namedtuple to provide that structure, assigning names
to values.

.. code:: python

    def convert(l):
        yield TextLine(line=l)
    df2 = df.map(convert)
    df2.take(1)



.. parsed-literal::

    [TextLine(line=u'Lorem ipsum dolor sit amet, consectetur adipiscing elit.')]



We can then save the DataFrame as a table. Note that this overwrites any
existing table with that name.

.. code:: python

    df2.to_table(table_name='gibberish')

.. parsed-literal::

    Deleted file:///tmp/spark/warehouse/gibberish




.. parsed-literal::

    <edx.idea.data_frame.DataFrame at 0x7f2dde515fd0>



.. code:: python

    res = sql_query('SELECT line FROM gibberish')

Again, this SQL query has not yet been executed, it is not until we
actually try to use the results that it is run.

.. code:: python

    res.take(10)



.. parsed-literal::

    [Row(line=u'Nullam ut enim in urna hendrerit luctus et ut leo.'),
     Row(line=u'Aenean lacinia metus a ipsum bibendum egestas.'),
     Row(line=u'Phasellus nec arcu dapibus, elementum ex sit amet, dapibus sem.'),
     Row(line=u'Integer feugiat magna eget urna porta, at faucibus eros molestie.'),
     Row(line=u'Nullam faucibus odio porttitor, fermentum felis eget, consectetur augue.'),
     Row(line=u'Vivamus in massa sed sem vulputate pellentesque.'),
     Row(line=u'Nunc nec orci eget purus ullamcorper auctor eget eu justo.'),
     Row(line=u'Maecenas nec turpis ac nisl pharetra condimentum at sed sem.'),
     Row(line=u'Proin viverra turpis at blandit sollicitudin.'),
     Row(line=u'Vestibulum et risus feugiat, molestie dolor nec, ultrices sapien.')]



Tables can be partitioned using a primary key. When writing to a
partioned table, only the modified partitions are overwritten.

Note that if a partition is written to, it is entirely overwritten, so
the replacement data must be complete.

.. code:: python

    PartitionedTextLine = namedtuple('TextLine', ['partition', 'line'])
.. code:: python

    def part_convert(l):
        yield PartitionedTextLine(partition=(len(l) % 5), line=l)
    part_df = df.map(part_convert)
.. code:: python

    part_df.to_table(table_name='partitioned', primary_key='partition')

.. parsed-literal::

    Deleted file:///tmp/spark/warehouse/partitioned/partition=3
    Deleted file:///tmp/spark/warehouse/partitioned/partition=2
    Deleted file:///tmp/spark/warehouse/partitioned/partition=4
    Deleted file:///tmp/spark/warehouse/partitioned/partition=1
    Deleted file:///tmp/spark/warehouse/partitioned/partition=0




.. parsed-literal::

    <edx.idea.data_frame.DataFrame at 0x7f2dde4cdc50>



Note the first few records of partition 0.

.. code:: python

    sql_query("SELECT line FROM partitioned WHERE partition=0 LIMIT 10").collect()



.. parsed-literal::

    [Row(line=u'Nullam ut enim in urna hendrerit luctus et ut leo.'),
     Row(line=u'Integer feugiat magna eget urna porta, at faucibus eros molestie.'),
     Row(line=u'Maecenas nec turpis ac nisl pharetra condimentum at sed sem.'),
     Row(line=u'Proin viverra turpis at blandit sollicitudin.'),
     Row(line=u'Vestibulum et risus feugiat, molestie dolor nec, ultrices sapien.'),
     Row(line=u'Quisque at ante faucibus, pellentesque velit elementum, ullamcorper tellus.'),
     Row(line=u'Cras et turpis non augue porta vehicula.'),
     Row(line=u'Etiam ac sem commodo, rutrum urna id, elementum turpis.'),
     Row(line=u'Nam eget quam bibendum, aliquam nunc vitae, consequat massa.'),
     Row(line=u'Etiam non justo convallis, sollicitudin erat vel, suscipit justo.')]



Now we create a new DataFrame containing records only in partition 0 and
convert all of the strings to upper case.

Note that when we call ``to_table`` on this DataFrame it only replaces
partition 0, all other partitions are untouched.

.. code:: python

    def part_convert_upper(l):
        part = len(l) % 5
        if part == 0:
            yield PartitionedTextLine(partition=part, line=l.upper())
    
    df.map(part_convert_upper).to_table(table_name='partitioned', primary_key='partition')

.. parsed-literal::

    Deleted file:///tmp/spark/warehouse/partitioned/partition=0




.. parsed-literal::

    <edx.idea.data_frame.DataFrame at 0x7f2dde4dd610>



Partition 0 records are now all uppercase.

.. code:: python

    sql_query("SELECT line FROM partitioned WHERE partition=0 LIMIT 10").collect()



.. parsed-literal::

    [Row(line=u'NULLAM UT ENIM IN URNA HENDRERIT LUCTUS ET UT LEO.'),
     Row(line=u'INTEGER FEUGIAT MAGNA EGET URNA PORTA, AT FAUCIBUS EROS MOLESTIE.'),
     Row(line=u'MAECENAS NEC TURPIS AC NISL PHARETRA CONDIMENTUM AT SED SEM.'),
     Row(line=u'PROIN VIVERRA TURPIS AT BLANDIT SOLLICITUDIN.'),
     Row(line=u'VESTIBULUM ET RISUS FEUGIAT, MOLESTIE DOLOR NEC, ULTRICES SAPIEN.'),
     Row(line=u'QUISQUE AT ANTE FAUCIBUS, PELLENTESQUE VELIT ELEMENTUM, ULLAMCORPER TELLUS.'),
     Row(line=u'CRAS ET TURPIS NON AUGUE PORTA VEHICULA.'),
     Row(line=u'ETIAM AC SEM COMMODO, RUTRUM URNA ID, ELEMENTUM TURPIS.'),
     Row(line=u'NAM EGET QUAM BIBENDUM, ALIQUAM NUNC VITAE, CONSEQUAT MASSA.'),
     Row(line=u'ETIAM NON JUSTO CONVALLIS, SOLLICITUDIN ERAT VEL, SUSCIPIT JUSTO.')]



Other partitions are still there and still lowercase.

.. code:: python

    sql_query("SELECT line FROM partitioned WHERE partition=1 LIMIT 10").collect()



.. parsed-literal::

    [Row(line=u'Aenean lacinia metus a ipsum bibendum egestas.'),
     Row(line=u'Quisque et nisi nec ipsum dictum lobortis at vitae urna.'),
     Row(line=u'Duis vitae erat tempus dui fringilla accumsan.'),
     Row(line=u'Donec porttitor neque at nulla rutrum blandit.'),
     Row(line=u'Nunc at est et leo mollis tristique.'),
     Row(line=u'Donec lobortis metus et mi dignissim suscipit.'),
     Row(line=u'Duis et mi nec erat elementum egestas vel ut nulla.'),
     Row(line=u'Integer et nunc non augue rutrum vulputate ut in tellus.'),
     Row(line=u'Fusce quis eros eu urna elementum efficitur id eget dui.'),
     Row(line=u'Morbi ut tortor eu felis gravida lacinia euismod ut dui.')]

Design
======

The central concept in this API is a "DataFrame". It represents a handle for an immutable data set. It can be transformed into other immutable data sets (DataFrames) using a variety of methods. DataFrames are conceptually similar to a data table, groups of records each of which having named fields (or columns). Abstractly they are meant to operate much like other in-memory data structures in python, however, they can perform large scale distributed computations on a cluster instead of local computation on the machine executing the script if they want/need to. This allows for simplified scaling from very small data sets to very large ones.

An "Engine" performs the actual computations on DataFrames and is capable of transforming from one to another.

The DataFrame concept is not new (RDD, SQL Table, pandas DataFrame etc), however, an explicit decision was made not to simply use some third party technology and instead provide an abstraction layer (DataFrame + Engine). The reasons for this decision are as follows:

1. We combine the strengths of different technologies as long as we transform the data appropriately in order to conform to the interface.
2. We can change direction and swap out technologies without having to rewrite our transformations.
3. We have an interface where we can shim different versions of the underlying technology stack, allowing us to update external dependencies without breaking the client code.
4. We are free to make performance optimizations as allowed by the interface.
5. The open source community and/or edX are free to use proprietary (and potentially expensive) technologies behind the interface.

DataFrame
---------

Transformations
~~~~~~~~~~~~~~~

Transformations can be executed lazily. They need not have the data available when they return a DataFrame, that DataFrame is simply a promise. When an action is executed, the promise must be resolved.

``map_reduce(map_generator, reduce_generator)``

``map_generator(record)``

``reduce_generator(key, values_iter)``

This function executes the ``map_generator`` over every record in the DataFrame. The ``map_generator`` can yield any number of tuples in the format (key, value). The tuples yielded from the ``map_generator`` are then grouped by key and passed as parameters to the ``reduce_generator`` as a key and an iterator to the set of values in that key group. The ``reduce_generator`` can yield arbitrary records which will be stored in the DataFrame returned by the ``map_reduce`` function.

The ``map_generator`` and ``reduce_generator`` may be executed in arbitrary processes and may be executed multiple times for the same inputs. They are expected to be idempotent and should have no side effects.

The ``reduce_generator`` can expect state to preserved throughout the entire processing of the iterator.


``sql_query(query)``

Execute a SQL query and return the result as a DataFrame. A subset of SQL queries is supported. The resulting DataFrame will contain records that are formatted as namedtuples where the resulting columns are fields in the tuple.


Actions
~~~~~~~

Actions require some or all of the data be available, any necessary computations are performed synchronously when calling an action.

``take(n_records)``

Returns a list of the first ``n_records`` of the DataFrame.

``each(each_function)``

``each_function(record)``

Executes ``each_function`` on each record in the DataFrame in order. This action is not parallizable and is guaranteed to be executed in an environment where state is preserved. It is intended for use to transfer records out of a DataFrame into another system. It could be used, for example, to build SQL transactions to insert into an RDBMS, or to write records out to a file.

``to_table(table_name, schema=None, primary_key=None, append=False)``

Saves the contents of the DataFrame into a table that can be queried using ``sql_query()``.

If the ``primary_key`` is specified, it is used as the partitioning key when saving the data. For each distinct value of this key found in the data, the entire partition associated with that key will be overwritten. This allows for selective updates to data within the table as long as entire partitions are overwritten.

If the primary_key is not specified, the entire table is overwritten.

If ``append == True`` data is appended to the relevant partition (or table) instead of overwriting it.

If the ``schema`` is specified the table is created with that schema and all data within the DataFrame must conform to it. If ``schema`` is not specified, the schema is inferred from the first record in the table. All other records in the DataFrame are assumed to have the same schema as that first record.

In order for data to be saved to a table using this method it must be stored in the DataFrame in such a way that the columns and values for those columns is apparent. This can be done by making every record a ``namedtuple`` or a tuple of tuples in the format ``((column_name, value), (other_column_name, other_value), ...)``, dictionaries are also supported.

Dependencies
============

* Python 2.7


Spark Engine Dependencies
-------------------------

* JDK 7+
* Spark 1.2.0
