{% materialization remote_table, adapter='clickhouse', supported_languages=['python', 'sql'] -%}
  {%- set remote_config = config.require('remote_config') -%}

  {%- set remote_cluster = remote_config.get('cluster') -%}
  {%- set remote_schema = remote_config.get('local_db_prefix') + this.schema -%}
  {%- set remote_identifier = this.identifier + remote_config.get('local_suffix') -%}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set remote_relation = target_relation.incorporate(path={"identifier": remote_identifier, "schema": remote_schema}, remote_cluster=remote_cluster) -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {%- set column_changes = none -%}
  {%- if existing_relation -%}
    {%- if sql is none -%}
        {%- set sql = clickhouse__create_select_query_from_schema() -%}
    {%- endif -%}
    {%- set column_changes = adapter.check_incremental_schema_changes('ignore', existing_relation, sql) -%}
    {%- set target_engine = {"cluster": remote_cluster, "database": remote_schema, "table": remote_identifier, "sharding_key": config.get("sharding_key", "rand()")} -%}
    {%- set engine_changes = adapter.check_distributed_engine_changes(existing_relation.engine, target_engine) -%}
  {%- endif -%}

  {% call statement('main') %}
    {%- if column_changes or engine_changes or existing_relation is none or should_full_refresh() -%}
      {{ create_distributed_table(target_relation, remote_relation, sql) }}
    {%- else -%}
      {{ log("no-op run: distributed table exists with correct schema.", info=true) }}
      select true;
    {%- endif -%}
  {% endcall %}

  {% set should_revoke = should_revoke(target_relation, full_refresh_mode) %}
  {% set grant_config = config.get('grants') %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  {{ adapter.commit() }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}