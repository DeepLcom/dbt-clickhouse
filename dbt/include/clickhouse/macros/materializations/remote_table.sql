{% materialization remote_table, adapter='clickhouse', supported_languages=['python', 'sql'] -%}
  {%- set remote_config = config.get('remote_config', none) -%}
  {%- if remote_config is none -%}
    {% do exceptions.raise_compiler_error('`remote_config` model configuration needs to be provided to run `remote_table` materialization.') %}
  {%- endif -%}

  {%- set remote_cluster = remote_config.get('cluster') -%}
  {%- set remote_schema = remote_config.get('local_db_prefix') + this.schema -%}
  {%- set remote_identifier = this.identifier + remote_config.get('local_suffix') -%}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set remote_relation = target_relation.incorporate(path={"identifier": remote_identifier, "schema": remote_schema}, remote_cluster=remote_cluster) -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set column_changes = none -%}
  {%- if existing_relation -%}
    {%- set column_changes = adapter.check_incremental_schema_changes('ignore', existing_relation, sql) -%}
  {%- endif -%}

  {% call statement('main') %}
    {%- if column_changes or existing_relation is none or should_full_refresh() -%}
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

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ adapter.commit() }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}