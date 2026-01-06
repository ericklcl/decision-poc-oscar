{#-- dbt run-operation load_raw_parquet_from_vars --#}
{% macro load_raw_parquet_from_vars(var_name='raw_json_copy') %}

  {%- set cfg = var(var_name) -%}
  {%- set stage_base = cfg['stage_base'] -%}
  {%- set file_format_name = cfg['file_format_name'] -%}

  {%- set force_raw = cfg.get('force_copy', false) -%}
  {%- set force = (force_raw | string | lower) in ['true','1','yes','y'] -%}

  {%- set record_source = cfg.get('record_source', none) -%}
  {%- set tables = cfg.get('tables', []) -%}

  {% if tables | length == 0 %}
    {{ exceptions.raise_compiler_error("No tables found in var '" ~ var_name ~ ".tables'") }}
  {% endif %}

  {{ log("Bulk COPY(PARQUET) start | stage_base=" ~ stage_base ~ " | file_format=" ~ file_format_name ~ " | force=" ~ (force | string), True) }}

  {%- for t in tables -%}
    {%- set source_name  = t.get('source_name') -%}
    {%- set target_table = t.get('target_table') -%}
    {%- set s3_folder    = t.get('s3_folder') -%}
    {%- set pattern      = t.get('pattern') -%}

    {% if not source_name or not target_table or not s3_folder %}
      {{ exceptions.raise_compiler_error(
        "Invalid config item in var '" ~ var_name ~ "'. Required: source_name, target_table, s3_folder. Got: " ~ (t | tojson)
      ) }}
    {% endif %}

    {%- set stage_name = stage_base ~ "/" ~ s3_folder -%}

    {{ log(
        "COPY(PARQUET) [" ~ (loop.index | string) ~ "/" ~ (tables | length | string) ~ "] "
        ~ "folder=" ~ s3_folder
        ~ " -> target=" ~ source_name ~ "." ~ target_table,
        True
    ) }}

    {{ custom_load_parquet_as_variant_from_stage(
        source_ref=source(source_name, target_table),
        stage_name=stage_name,
        file_format_name=file_format_name,
        force=force,
        file_pattern=pattern,
        record_source=record_source
    ) }}

  {%- endfor -%}

  {{ log("Bulk COPY(PARQUET) finished", True) }}

{% endmacro %}
