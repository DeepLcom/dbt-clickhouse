import os
import pytest
from dbt.tests.util import run_dbt

base_model = """
{{
    config(
        materialized='%s',
        order_by='number',
        unique_key='number',
        add_to_remote_clusters=True,
    )
}}
select number from numbers(3)
"""


class TestRemoteDistributedTable:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": base_model % "incremental",
            "distributed_incremental_model.sql": base_model % "distributed_incremental",
            "table_model.sql": base_model % "table",
            "distributed_table_model.sql": base_model % "distributed_table",
            "view_model.sql": base_model % "view",
            "materialized_view_model.sql": base_model % "materialized_view",
        }

    @pytest.fixture(scope="class")
    def test_config(self, test_config):
        """Chaining test_config fixture to modify db_engine for this test."""
        test_config["db_engine"] = "Replicated('/clickhouse/databases/{uuid}', '{shard}', '{replica}')"
        return test_config

    @pytest.fixture(scope="class")
    def remote_hosts(self, project):
        remote_clusters = project.test_config['remote_clusters']
        hosts = project.run_sql(
            f"select host_name from system.clusters where cluster in ('{"','".join(remote_clusters)}')",
            fetch="all",
        )
        assert len(hosts[0]) >= 1
        return hosts[0]

    @pytest.mark.parametrize(
        "model",
        (
            "incremental_model",
            # "distributed_incremental_model",
            # "table_model",
            # "distributed_table_model",
            # "view_model",
            # "materialized_view_model",
        )
    )
    @pytest.mark.skipif(
        os.environ.get('DBT_CH_TEST_REMOTE_CLUSTERS', '').strip() == '', reason='No remote clusters'
    )
    def test_replicated_db(self, project, remote_hosts, model):
        run_dbt(["run", "--select", model])
        for host in remote_hosts:
            # check for correct table creation
            result = project.run_sql(
                f"select engine from remote('{host}','system.tables') where name='{model}'",
                fetch="one"
            )
            assert result is not None
            assert result[0] == "Distributed"
            # check for correct data query results
            result = project.run_sql(
                f"select number from remote('{host}','{project.test_schema}','{model}') order by number",
                fetch="all",
            )
            assert len(result[0]) == 3
            assert result[0] == [0, 1, 2]
