from dbt.contracts.graph.nodes import BaseNode
from dbt.flags import get_flags

from dbt.adapters.base import AdapterPlugin
from dbt.adapters.clickhouse.column import ClickHouseColumn  # noqa
from dbt.adapters.clickhouse.connections import ClickHouseConnectionManager  # noqa
from dbt.adapters.clickhouse.credentials import ClickHouseCredentials
from dbt.adapters.clickhouse.impl import ClickHouseAdapter
from dbt.adapters.clickhouse.logger import logger
from dbt.adapters.clickhouse.relation import ClickHouseRelation  # noqa
from dbt.include import clickhouse  # noqa

Plugin = AdapterPlugin(
    adapter=ClickHouseAdapter,
    credentials=ClickHouseCredentials,
    include_path=clickhouse.PACKAGE_PATH,
)


def get_materialization(self):
    flags = get_flags()
    materialized = flags.vars.get('materialized') or self.config.materialized
    is_distributed = self.config.extra.get('is_distributed')
    if materialized in ('incremental', 'table') and is_distributed:
        materialized = f"distributed_{materialized}"
    return materialized


# patches a BaseNode method to allow setting `materialized` config overrides via dbt flags
BaseNode.get_materialization = get_materialization
