import pandas
import pytest
import time

from pyflink.common import Configuration
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.descriptors import Schema

from src.utils.debug import *


@pytest.fixture(scope="session")
def table_env(request) -> TableEnvironment:
    env_settings = EnvironmentSettings.in_batch_mode()
    # env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    return table_env


def test_json_query(table_env):
    # Define the source table
    # {'a': [1, 2, 3, 4], 'b': 1}
    # table = table_env.from_elements(
    #     [([1, 2, 3, 4], 1), ([], 2)],
    #     DataTypes.ROW(
    #         [
    #             DataTypes.FIELD("a", DataTypes.ROW()),
    #             DataTypes.FIELD("b", DataTypes.INT())
    #         ]
    #     ),
    # )
    # table_env.create_temporary_view("source_table", table)

    dataframe = pandas.DataFrame(
        {
            "js": [
                '{"a": [1,2,3,4], "b": 1}',
                '{"a":null,"b":2}',
                '{"a":"foo", "c":null}',
                "null",
                "[42,47,55]",
                "[]",
            ]
        }
    )
    table = table_env.from_pandas(dataframe)
    table_env.create_temporary_view("source_table", table)

    # Define the sink table
    table_env.execute_sql("""
        CREATE TABLE sink_table (
            res STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Define a transformation
    table_result = table_env.execute_sql("""
        INSERT INTO sink_table
        SELECT JSON_QUERY(source_table.`js`, '$.a') AS `res`
        FROM source_table
    """)
    table_result.print()


def test_json_query_w_conditional_array_wrapper(table_env):
    dataframe = pandas.DataFrame(
        {
            "js": [
                '{"a": [1,2,3,4], "b": 1}',
                '{"a":null,"b":2}',
                '{"a":"foo", "c":null}',
                "null",
                "[42,47,55]",
                "[]",
            ]
        }
    )
    table = table_env.from_pandas(dataframe)
    table_env.create_temporary_view("source_table", table)

    # Define the sink table
    table_env.execute_sql("""
        CREATE TABLE sink_table (
            res STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Define a transformation
    table_result = table_env.execute_sql("""
        INSERT INTO sink_table
        SELECT JSON_QUERY(source_table.`js`, '$.a' WITH CONDITIONAL ARRAY WRAPPER) AS `res`
        FROM source_table
    """)
    table_result.print()
