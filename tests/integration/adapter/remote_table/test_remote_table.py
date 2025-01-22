import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

model = """
{{
    config(
        materialized='remote_table',
        remote_config={'cluster': 'test_shard', 'local_db_prefix': '_', 'local_suffix': '_local'},
        sharding_key='key1',
    )
}}
select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
"""


class TestRemoteTableRemoteConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "remote_table.sql": model,
        }

    def test_with_remote_configuration(self, project):
        # initialize a local table
        project.run_sql(f"create database if not exists _{project.test_schema} on cluster test_shard")
        project.run_sql(f"""
            create table _{project.test_schema}.remote_table_local on cluster test_shard
            (key1 UInt64, key2 Int64)
            engine=MergeTree order by key1
        """)

        # the created distributed table should point to a local table as defined in the model's `remote_config`
        run_dbt()

        # insert data via distributed table
        project.run_sql(f"""
            insert into {project.test_schema}.remote_table
            select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
        """)

        _assert_is_distributed_table(project)
        _assert_correct_engine(project)
        _assert_correct_data(project)

        # rerun (should be no-op)
        _, log_output = run_dbt_and_capture()
        assert "no-op run" in log_output


def _assert_is_distributed_table(project):
    # check correct table creation on current host
    result = project.run_sql(
        f"select engine from system.tables where name='remote_table'",
        fetch="one"
    )
    assert result is not None
    assert result[0] == "Distributed"


def _assert_correct_engine(project):
    # assert correct engine parameters
    result = project.run_sql(f"select create_table_query from system.tables where name='remote_table'", fetch="one")
    assert f"Distributed('test_shard', '_{project.test_schema}', 'remote_table_local', key1)" in result[0]


def _assert_correct_data(project):
    # query remote data from distributed table
    result = project.run_sql("select count(*) as num_rows from remote_table", fetch="one")
    assert result[0] == 10
