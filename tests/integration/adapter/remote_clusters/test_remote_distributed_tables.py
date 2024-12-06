import pytest
from dbt.tests.util import run_dbt

base_model = """
{{
    config(
        materialized='%s',
        order_by='number',
        unique_key='number',
        add_to_remote_cluster=True,
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
    def hosts(self, project):
        remote_clusters = project.test_config['remote_clusters']
        hosts = project.run_sql(
            f"select host_name from system.clusters where cluster in (`{"`,`".join(remote_clusters)}`)",
            fetch="all",
        )
        assert len(hosts) >= 1
        return hosts

    @pytest.mark.parametrize(
        "model",
        (
            "incremental_model",
            "distributed_incremental_model",
            "table_model",
            "distributed_table_model",
            "view_model",
            "materialized_view_model",
        )
    )
    def test_incremental(self, project, hosts, model):
        run_dbt(["run", "--select", model])
        for host in hosts:
            # check for correct table engine
            result = project.run_sql(
                f"select engine from remote('{host}','system.tables' where name='{model}')",
                fetch="one"
            )
            assert result[0] == "Distributed"
            # check for correct data query results
            result = project.run_sql(
                f"select number from remote('{host}','{model}') order by number",
                fetch="all",
            )
            assert len(result[0]) == 3
            assert result[0] == [1, 2, 3]
