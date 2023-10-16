import time

from pyflink.common import Configuration
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.descriptors import Schema

from src.utils.debug import *


def get_table_env() -> TableEnvironment:
    env_settings = EnvironmentSettings.in_batch_mode()
    # env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table_config = table_env.get_config()
    config = Configuration()
    config.set_string("parallelism.default", "1")
    table_config.add_configuration(config)

    return table_env


def run_job_1(table_env: TableEnvironment):
    # Define the source table
    table = table_env.from_elements(
        [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        DataTypes.ROW(
            [
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("name", DataTypes.STRING())
            ]
        ),
    )
    table_env.create_temporary_view("source_table_1", table)

    # Define the sink table
    table_env.execute_sql("""
        CREATE TABLE sink_table_1 (
            id INT,
            name STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Define a transformation
    table_result = table_env.execute_sql("""
        INSERT INTO sink_table_1
        SELECT id, UPPER(name) as name
        FROM source_table_1
    """)
    table_result.print()


def run_job_2(table_env: TableEnvironment):
    # Define the source table
    table = table_env.from_elements(
        [(1, 25), (2, 30), (3, 22)],
        DataTypes.ROW(
            [
                DataTypes.FIELD("id", DataTypes.INT()),
                DataTypes.FIELD("age", DataTypes.INT())
            ]
        ),
    )
    table_env.create_temporary_view("source_table_2", table)


    # Define the sink table
    table_env.execute_sql("""
        CREATE TABLE sink_table_2 (
            id INT,
            age INT
        ) WITH (
            'connector' = 'print'
        )
    """)

    # Define a transformation
    table_result = table_env.execute_sql("""
        INSERT INTO sink_table_2
        SELECT id, age * 2 as age
        FROM source_table_2
    """)

    table_result.print()


if __name__ == '__main__':
    table_env = get_table_env()
    run_job_1(table_env)
    run_job_2(table_env)

    # log(INFO, "Sleeping ...")
    # time.sleep(1000)
