import pytest
from dbt.tests.util import run_dbt


model_with_defaults = """
{{
    config(
        materialized='remote_table',
    )
}}
select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
"""


class TestRemoteTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "remote_table.sql": model_with_defaults,
        }

    def test_with_defaults(self, project):
        # initialize a local table on current cluster
        project.run_sql(f"""
            create table {project.test_schema}.remote_table_local on cluster {project.test_config["cluster"]}
            (key1 UInt64, key2 Int64)
            engine=ReplicatedMergeTree order by key1
        """)

        # this `remote_table` run with default configuration will point to previously created local table, taking
        # `local_db_prefix` and `local_suffix` settings into account.
        run_dbt()

        # insert into distributed table
        project.run_sql(f"""
            insert into {project.test_schema}.remote_table
            select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
            settings insert_quorum=1
        """)

        _assert_is_distributed_table(project)
        _assert_correct_distributed_data(project)


model_with_remote_configuration = """
{{
    config(
        materialized='remote_table',
        remote_config={'cluster': 'test_remote', 'schema': this.schema, 'identifier': 'remote_target_table'},
        sharding_key='key1',
    )
}}
select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
"""


class TestRemoteTableRemoteConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "remote_table.sql": model_with_remote_configuration,
        }

    def test_with_remote_configuration(self, project):
        # initialize a local table on remote cluster 'test_remote_cluster'
        project.run_sql(f"create database if not exists {project.test_schema} on cluster `test_remote`")
        project.run_sql(f"""
            create table {project.test_schema}.remote_target_table on cluster `test_remote`
            engine=MergeTree order by key1
            as select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
        """)

        # the created distributed table should point to a local table as defined in the model's `remote_config`
        run_dbt()

        _assert_is_distributed_table(project)
        _assert_correct_distributed_data(project)

        # assert correct engine parameters
        result = project.run_sql(f"select create_table_query from system.tables where name='remote_table'", fetch="one")
        assert f"Distributed('test_remote', '{project.test_schema}', 'remote_target_table', key1)" in result[0]


def _assert_is_distributed_table(project):
    # check correct table creation on current host
    result = project.run_sql(
        f"select engine from system.tables where name='remote_table'",
        fetch="one"
    )
    assert result is not None
    assert result[0] == "Distributed"


def _assert_correct_distributed_data(project):
    # query remote data from distributed table
    result = project.run_sql("select count(*) as num_rows from remote_table", fetch="one")
    assert result[0] == 10
