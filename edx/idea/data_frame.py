

from edx.idea.plugin import PluginManager


class DataFrame(object):

    def __init__(self):
        self.engine = PluginManager().engine

    def map(self, map_function):
        return self.engine.map(self, map_function)

    def map_reduce(self, map_function, reduce_function):
        return self.engine.map_reduce(self, map_function, reduce_function)

    def filter(self, filter_function):
        return self.engine.filter(self, filter_function)

    def take(self, n_records):
        return self.engine.take(self, n_records)

    def collect(self):
        return self.engine.collect(self)

    def each(self, each_function):
        return self.engine.each(self, each_function)

    def count(self):
        return self.engine.count(self)

    def to_table(self, table_name=None, schema=None, primary_key=None):
        return self.engine.to_table(self, table_name=table_name, schema=schema, primary_key=primary_key)

    def cache(self):
        return self.engine.cache(self)

    @staticmethod
    def from_sql_query(query):
        return PluginManager().engine.from_sql_query(query)

    @staticmethod
    def from_table(table_name):
        return PluginManager().engine.from_table(table_name)

    @staticmethod
    def from_url(file_url):
        return PluginManager().engine.from_url(file_url)

    @staticmethod
    def from_list(data):
        return PluginManager().engine.from_list(data)
