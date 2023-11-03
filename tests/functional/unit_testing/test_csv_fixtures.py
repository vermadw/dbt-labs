import pytest
from dbt.exceptions import ParsingError, YamlParseDictError
from dbt.tests.util import run_dbt, write_file
from .fixtures import (
    my_model_sql,
    my_model_a_sql,
    my_model_b_sql,
    test_my_model_csv_yml,
    datetime_test,
    datetime_test_invalid_format,
    datetime_test_invalid_format2,
)


class BaseUnitTestsCSV:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_csv_yml + datetime_test,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"my_test": "my_test_var"}}

    def test_basic(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        # Select by model name
        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5

        # Check error with invalid format
        write_file(
            test_my_model_csv_yml + datetime_test_invalid_format,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(YamlParseDictError):
            results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)

        # Check error with format not matching rows
        write_file(
            test_my_model_csv_yml + datetime_test_invalid_format2,
            project.project_root,
            "models",
            "test_my_model.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)


class TestUnitTestsWithInlineCSV(BaseUnitTestsCSV):
    pass
