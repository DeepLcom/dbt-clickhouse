{% materialization remote_table, adapter='clickhouse', supported_languages=['python', 'sql'] -%}
  {%- set remote_config = config.get('remote_config', {}) -%}
  {%- set remote_cluster = remote_config.get('cluster') or adapter.get_clickhouse_cluster_name() -%}
  {%- set remote_schema = remote_config.get('schema') or adapter.get_clickhouse_local_db_prefix() + this.schema -%}
  {%- set remote_identifier = remote_config.get('identifier') or this.identifier + adapter.get_clickhouse_local_suffix() -%}

  {% set target_relation = this.incorporate(type='table') %}
  {% set remote_relation = target_relation.incorporate(path={"identifier": remote_identifier, "schema": remote_schema}, remote_cluster=remote_cluster) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% call statement('main') %}
    {{ create_distributed_table(target_relation, remote_relation, sql) }}
  {% endcall %}

  {% set should_revoke = should_revoke(target_relation, full_refresh_mode) %}
  {% set grant_config = config.get('grants') %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}