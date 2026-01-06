{% macro custom_load_parquet_as_variant_from_stage(
    source_ref,
    stage_name,
    file_format_name,
    force=false,
    file_pattern=none,
    record_source=none
) %}

  {%- set db = source_ref.database -%}
  {%- set sch = source_ref.schema -%}
  {%- set ident = source_ref.identifier -%}

  {%- set force_sql = 'TRUE' if (force | string | lower) in ['true','1','yes','y'] else 'FALSE' -%}

  {%- if record_source is not none -%}
    {%- set record_source_esc = (record_source | string) | replace("'", "''") -%}
  {%- endif -%}

  {{ log(
      "COPY(PARQUET) -> " ~ db ~ "." ~ sch ~ "." ~ ident
      ~ " | stage=" ~ stage_name
      ~ " | file_format=" ~ file_format_name
      ~ " | force=" ~ force_sql
      ~ ("" if file_pattern is none else " | pattern=" ~ file_pattern)
      ~ ("" if record_source is none else " | record_source=" ~ record_source),
      True
  ) }}

  {% call statement('custom_load_parquet_as_variant_copy', fetch_result=true, auto_begin=true) %}
    COPY INTO {{ db }}.{{ sch }}.{{ ident }}
      (json_data, s3_file_name, s3_file_row_number, s3_file_last_modified, load_timestamp_utc, record_source)
    FROM (
      SELECT
        $1                                         AS json_data,
        metadata$filename                          AS s3_file_name,
        metadata$file_row_number                   AS s3_file_row_number,
        metadata$file_last_modified                AS s3_file_last_modified,
        CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS load_timestamp_utc,
        {%- if record_source is not none -%}
        '{{ record_source_esc }}'
        {%- else -%}
        NULL
        {%- endif -%}                              AS record_source
      FROM {{ stage_name }}
      (
        file_format => {{ file_format_name }}
        {%- if file_pattern is not none %}
        , pattern => '{{ file_pattern }}'
        {%- endif %}
      )
    )
    FORCE = {{ force_sql }}
    ;
  {% endcall %}

  {%- set copy_res = load_result('custom_load_parquet_as_variant_copy') -%}
  {%- if copy_res is not none and copy_res['table'] is not none -%}
    {%- for r in copy_res['table'].rows -%}
      {{ log(
          "COPY file=" ~ (r[0] | string)
          ~ " | status=" ~ (r[1] | string)
          ~ " | rows_parsed=" ~ (r[2] | string)
          ~ " | rows_loaded=" ~ (r[3] | string),
          True
      ) }}
    {%- endfor -%}
  {%- else -%}
    {{ log("COPY returned no result set to log.", True) }}
  {%- endif -%}

{% endmacro %}
