
from edx.idea.data_frame import DataFrame


def sql_query(query):
    return DataFrame.from_sql_query(query)
