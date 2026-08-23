{#
    Ghi đè cách dbt đặt tên dataset đích.

    Mặc định dbt nối: <dataset trong profile>_<schema khai báo ở model>
    -> ở dev sẽ ra dbt_dev_staging, dbt_dev_marts. Rất tốt cho dev vì mỗi
    người có bộ dataset riêng, không đụng nhau.

    Nhưng ở prod ta muốn tên sạch: analytics_staging, analytics_marts
    thay vì analytics_analytics_marts. Macro này xử lý đúng điều đó.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- elif target.name == 'prod' -%}

        {#- Prod: dùng thẳng tên schema khai báo trong dbt_project.yml -#}
        {{ custom_schema_name | trim }}

    {%- else -%}

        {#- Dev: thêm tiền tố dataset cá nhân để cô lập môi trường -#}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
