import pytest
from dbt.tests.util import run_dbt

unit_tests_yml = """
unit_tests:
  - name: test_valid_email_address # this is the unique name of the test
    description: my favorite unit test
    model: dim_wizards # name of the model I'm unit testing
    given: # the mock data for your inputs
      - input: ref('stg_wizards')
        rows:
          - {WIZARD_ID: "1", EMAIL: cool@example.com,     EMAIL_TOP_LEVEL_DOMAIN: example.com}
          - {WIZARD_ID: "2", EMAIL: cool@unknown.com,     EMAIL_TOP_LEVEL_DOMAIN: unknown.com}
          - {WIZARD_ID: "3", EMAIL: badgmail.com,         EMAIL_TOP_LEVEL_DOMAIN: gmail.com}
          - {WIZARD_ID: "4", EMAIL: missingdot@gmailcom,  EMAIL_TOP_LEVEL_DOMAIN: gmail.com}
      - input: ref('top_level_email_domains')
        rows:
          - {TLD: example.com}
          - {TLD: gmail.com}
      - input: ref('stg_worlds')
        rows: []
    expect: # the expected output given the inputs above
      rows:
        - {WIZARD_ID: "1", IS_VALID_EMAIL_ADDRESS: true}
        - {WIZARD_ID: "2", IS_VALID_EMAIL_ADDRESS: false}
        - {WIZARD_ID: "3", IS_VALID_EMAIL_ADDRESS: false}
        - {WIZARD_ID: "4", IS_VALID_EMAIL_ADDRESS: false}
"""

stg_wizards_sql = """
select
    1 as wizard_id,
    'tom' as wizard_name,
    'cool@example.com' as email,
    '999-999-9999' as phone_number,
    1 as world_id,
    'example.com' as email_top_level_domain
"""

stg_worlds_sql = """
select
    1 as world_id,
    'Erewhon' as world_name
"""

top_level_email_domains_seed = """tld
gmail.com
yahoo.com
hocuspocus.com
dbtlabs.com
hotmail.com
"""

dim_wizards_sql = """
with wizards as (

    select * from {{ ref('stg_wizards') }}

),

worlds as (

    select * from {{ ref('stg_worlds') }}

),

accepted_email_domains as (

    select * from {{ ref('top_level_email_domains') }}

),

check_valid_emails as (

    select  
        wizards.wizard_id,
        wizards.wizard_name,
        wizards.email,
        wizards.phone_number,
        wizards.world_id,

		coalesce (regexp_like(
            wizards.email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        )
        = true
        and accepted_email_domains.tld is not null,
        false) as is_valid_email_address

    from wizards
    left join accepted_email_domains
        on wizards.email_top_level_domain = lower(accepted_email_domains.tld)

)

select
    check_valid_emails.wizard_id,
    check_valid_emails.wizard_name,
    check_valid_emails.email,
    check_valid_emails.is_valid_email_address,
    check_valid_emails.phone_number,
    worlds.world_name
from check_valid_emails
left join worlds
    on check_valid_emails.world_id = worlds.world_id
"""

schema_yml = """
models:
  - name: dim_wizards
    columns:
      - name: wizard_id 
      - name: wizard_name
      - name: email
      - name: phone_number
      - name: world_name
"""

class TestUnitTest:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "dim_wizards.sql": dim_wizards_sql,
            "stg_wizards.sql": stg_wizards_sql,
            "stg_worlds.sql": stg_worlds_sql,
            "schema.yml": schema_yml,
            "unit_tests.yml": unit_tests_yml,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "top_level_email_domains.csv": top_level_email_domains_seed
        }

    def test_dim_wizards(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["run"])
        assert len(results) == 3

        results = run_dbt(["test"])
