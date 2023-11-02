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


def test_cast_as_row(table_env):
    table_env.execute_sql("""
        SELECT CAST((12, 'Hello> world') AS ROW<a INT, b STRING>).`a` AS `a`
    """)

    # Define the sink table
    table_env.execute_sql("""
        CREATE TABLE sink_table (
            a INT
            b STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    table_result = table_env.execute_sql("""
        INSERT INTO sink_table
        SELECT JSON_QUERY(source_table.`js`, '$.a') AS `res`
        FROM source_table
    """)
    table_result.print()

    # table_result = table_env.execute_sql("CREATE TABLE table_as AS SELECT * FROM regular_table")
    # table_result.print()


def test_map_from_arrays(table_env):
    # table_env.execute_sql("""
    #     SELECT MAP_FROM_ARRAYS(ARRAY['a', 'b', 'c'], ARRAY['ghi', 'def', 'abc'])
    # """)

    # Define the sink table
    # table_env.execute_sql("""
    #     CREATE TABLE sink_table (
    #         s STRING
    #     ) WITH (
    #         'connector' = 'print'
    #     )
    # """)

    table_env.execute_sql("""
        CREATE TABLE sink_table (
            i INT,
            s STRING
        );
    """)

    # table_result = table_env.execute_sql("""
    #     INSERT INTO sink_table
    #     SELECT MAP_FROM_ARRAYS(ARRAY['a', 'b', 'c'], ARRAY['ghi', 'def', 'abc'])
    # """)
    # table_result.print()
