import pytest
import time

from pyflink.common import Configuration
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.descriptors import Schema

from src.utils.debug import *


@pytest.fixture(scope="session")
def minicluster(request):
    env_settings = EnvironmentSettings.in_batch_mode()
    # env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)

    table_config = table_env.get_config()
    table_config.get_configuration().set_boolean("execution.shutdown-on-attached-job-finished", False)
    table_config.get_configuration().set_boolean("execution.shutdown-on-application-finish", False)
    table_config.get_configuration().set_boolean("execution.shutdown-on-attached-exit", False)

    # Set config variables in another way, just to make sure...
    config = Configuration()
    config.set_string("parallelism.default", "1")
    config.set_boolean("execution.shutdown-on-attached-job-finished", False)
    config.set_boolean("execution.shutdown-on-application-finish", False)
    config.set_boolean("execution.shutdown-on-attached-exit", False)
    table_config.add_configuration(config)

    # # Add teardown code to stop the MiniCluster when the test session is finished
    # def close_minicluster():
    #     table_env.get_execution_environment().get_mini_cluster().close()
    # request.addfinalizer(close_minicluster)

    return table_env


def test_job_1(minicluster):
    table_env = minicluster

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

    # log(INFO, "Sleeping ...")
    # time.sleep(1000)

    # TODO (mehmet): Why is this not working?
    # with table_result.collect() as results:
    #     for i, result in enumerate(results):
    #         print(f"i= {i}, result= {result}")

    # Execute the job
    # table_env.execute("job_1")


def test_job_2(minicluster):
    table_env = minicluster

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
