local_dependency__dbt_project_yml = """

name: 'local_dep'
version: '1.0'
config-version: 2

profile: 'default'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

require-dbt-version: '>=0.1.0'

target-path: "target"  # directory which will store compiled SQL files
clean-targets:         # directories to be removed by `dbt clean`
    - "target"
    - "dbt_packages"


seeds:
  quote_columns: False

"""

local_dependency__models__schema_yml = """
version: 2
sources:
  - name: seed_source
    schema: "{{ var('schema_override', target.schema) }}"
    tables:
      - name: "seed"
        columns:
          - name: id
            tests:
              - unique

"""

local_dependency__models__model_to_import_sql = """
select * from {{ ref('seed') }}

"""

local_dependency__macros__dep_macro_sql = """
{% macro some_overridden_macro() -%}
100
{%- endmacro %}

"""

local_dependency__seeds__seed_csv = """id
1
"""

model_one_sql = """
select 1 as fun

"""

model_two_sql = """
select 1 as notfun

"""
