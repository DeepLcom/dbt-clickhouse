import json
import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture, get_connection

model = """
{{
    config(
        remote_config={'cluster': 'test_shard', 'local_db_prefix': '_', 'local_suffix': var('local_suffix')},
        sharding_key='key1',
        on_schema_change='sync_all_columns'
    )
}}

{% if adapter.get_relation(this.database, this.schema, this.table) is none %}
select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
{% else %}
select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
{% endif %}
"""


class TestRemoteTableRemoteConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "remote_table.sql": model,
        }

    @pytest.fixture(scope="class")
    def init_local_table(self, project):
        project.run_sql(f"create database if not exists _{project.test_schema} on cluster test_shard")
        project.run_sql(f"""
            create table _{project.test_schema}.remote_table_local on cluster test_shard
            (key1 UInt64, key2 Int64)
            engine=MergeTree order by key1
        """)
        return

    def test_with_remote_configuration(self, project, init_local_table):
        # the created distributed table should point to a local table as defined in the model's `remote_config`
        materialized_var = {"materialized": "remote_table"}
        local_suffix_var = {"local_suffix": "_lkjashdkoljasd"}
        run_dbt(["run", "--vars", json.dumps(materialized_var | local_suffix_var)])
        # run twice to fix any pre-existing distributed table (schema changes, engine clause)
        local_suffix_var = {"local_suffix": "_local"}
        run_dbt(["run", "--vars", json.dumps(materialized_var | local_suffix_var)])

        project.run_sql(f"""
            insert into {project.test_schema}.remote_table
            select toUInt64(number) as key1, toInt64(-number) as key2 from numbers(10)
        """)

        self._assert_is_distributed_table(project)
        self._assert_correct_engine(project)
        self._assert_correct_data(project)

        # rerun (should be no-op)
        _, log_output = run_dbt_and_capture(["run", "--vars", json.dumps(materialized_var | local_suffix_var)])
        assert "no-op run" in log_output

    @staticmethod
    def _assert_is_distributed_table(project):
        # check correct table creation on current host
        result = project.run_sql(
            f"select engine from system.tables where name='remote_table'",
            fetch="one"
        )
        assert result is not None
        assert result[0] == "Distributed"

    @staticmethod
    def _assert_correct_engine(project):
        # assert correct engine parameters
        result = project.run_sql(f"select create_table_query from system.tables where name='remote_table'", fetch="one")
        assert f"Distributed('test_shard', '_{project.test_schema}', 'remote_table_local', key1)" in result[0]

    @staticmethod
    def _assert_correct_data(project):
        # query remote data from distributed table
        result = project.run_sql("select count(key2) as num_rows from remote_table", fetch="one")
        assert result[0] == 10


class TestRemoteTableRemoteConfigReplicatedDB(TestRemoteTableRemoteConfig):
    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        test_config["db_engine"] = "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')"
        return test_config

    @pytest.fixture(scope="class")
    def init_local_table(self, project):
        schema_name = f"_{project.test_schema}"
        with get_connection(project.adapter):
            relation = project.adapter.Relation.create(database=project.database, schema=schema_name)
            project.adapter.create_schema(relation)
            project.created_schemas.append(schema_name)

        project.run_sql(f"""
            create table _{project.test_schema}.remote_table_local
            (key1 UInt64, key2 Int64)
            engine=MergeTree order by key1
        """)
