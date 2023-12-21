import pytest
from dbt.tests.util import run_dbt, get_unique_ids_in_results

from tests.functional.unit_testing.fixtures import (
    test_my_model_versioned_yml,
    test_my_model_versioned_unit_tests_yml,
    my_model_v1_sql,
    my_model_v2_sql,
    my_model_v3_sql,
    my_model_a_sql,
    my_model_b_sql,
)


# test with no version specified, should create a separate unit test for each version
class TestUnitTestingAllVersions:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "my_model_v1.sql": my_model_v1_sql,
            "my_model_v2.sql": my_model_v2_sql,
            "my_model_v3.sql": my_model_v3_sql,
            "schema.yml": test_my_model_versioned_yml,
            "unit_tests.yml": test_my_model_versioned_unit_tests_yml,
        }

    @pytest.fail("Not implemented")
    def test_unit_test_in_dependency(self, project):
        results = run_dbt(["build"])
        assert len(results) == 2

        results = run_dbt(["test"])
        assert len(results) == 3
        unique_ids = get_unique_ids_in_results(results)
        assert "unit_test.local_dep.dep_model.test_dep_model_id" in unique_ids


# with with an exclude version specified, should create a separate unit test for each version except the excluded version

# test with an include version specified, should create a single unit test for only the version specified

# test with an include and exclude version specified, should get ValidationError

# test with an include for an unverioned model, should error

# partial parsing test: test with no version specified, then add and exclude version, then switch to include version and make sure the right unit tests are generated for each

# test with no version specified in the schema file and use selection logic for a specific version

# test specifying the fixture version with {{ ref(name, version) }}
