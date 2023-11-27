import pandas
import pytest
import time

from pyflink.common import Configuration
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.descriptors import Schema

from src.utils.debug import *


@pytest.fixture(scope="session")
def table_env(request) -> TableEnvironment:
    # env_settings = EnvironmentSettings.in_batch_mode()
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    return table_env


def test_create_table_as(table_env):
    # Define the source table
    table_env.execute_sql("""
        CREATE TABLE source_table (
            a INT,
            b INT
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///tmp',
            'format' = 'csv'
        )
    """)
    table_env.execute_sql(
        "INSERT INTO source_table VALUES (35, 1), (32, 2)"
    )

    # Define the sink table
    table_env.execute_sql("""
        CREATE TABLE sink_table (
            a INT,
            b INT
        ) WITH (
            'connector' = 'print'
        )
    """)
    table_result = table_env.execute_sql("""
        INSERT INTO sink_table
        SELECT * FROM source_table
    """)

    # TODO: The following leads to "TableException: Failed to wait job finish"
    # table_result.print()

    # for result in table_result.collect():
    #     print(result)


def test_create_table_without_connector(table_env):
    # Define the source table
    # table_env.execute_sql("""
    #     CREATE TABLE source_table (
    #         a INT,
    #         b INT
    #     );
    # """)
    table_env.execute_sql("""
        CREATE TEMPORARY TABLE IF NOT EXISTS source_table (
            a INT,
            b INT
        );
    """)

    table_env.execute_sql(
        "INSERT INTO source_table VALUES (35, 1), (32, 2)"
    )

    # Define the sink table
    table_env.execute_sql("""
        CREATE TABLE sink_table (
            a INT,
            b INT
        ) WITH (
            'connector' = 'print'
        )
    """)
    table_result = table_env.execute_sql("""
        INSERT INTO sink_table
        SELECT * FROM source_table
    """)
