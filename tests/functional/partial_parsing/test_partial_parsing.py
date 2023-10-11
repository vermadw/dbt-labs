from argparse import Namespace
import pytest

import dbt.flags as flags
from dbt.tests.util import (
    run_dbt,
    write_file,
    rename_dir,
)
from tests.functional.utils import up_one
from dbt.tests.fixtures.project import write_project_files
from tests.functional.partial_parsing.fixtures import (
    model_one_sql,
    model_two_sql,
    local_dependency__dbt_project_yml,
    local_dependency__models__schema_yml,
    local_dependency__models__model_to_import_sql,
    local_dependency__macros__dep_macro_sql,
    local_dependency__seeds__seed_csv,
)

import os

os.environ["DBT_PP_TEST"] = "true"


class TestPortablePartialParsing:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return {"packages": [{"local": "local_dependency"}]}

    @pytest.fixture(scope="class")
    def local_dependency_files(self):
        return {
            "dbt_project.yml": local_dependency__dbt_project_yml,
            "models": {
                "schema.yml": local_dependency__models__schema_yml,
                "model_to_import.sql": local_dependency__models__model_to_import_sql,
            },
            "macros": {"dep_macro.sql": local_dependency__macros__dep_macro_sql},
            "seeds": {"seed.csv": local_dependency__seeds__seed_csv},
        }

    def rename_project_root(self, project, new_project_root):
        with up_one(new_project_root):
            rename_dir(project.project_root, new_project_root)
            project.project_root = new_project_root
            # flags.project_dir is set during the project test fixture, and is persisted across run_dbt calls,
            # so it needs to be reset between invocations
            flags.set_from_args(Namespace(PROJECT_DIR=new_project_root), None)

    @pytest.fixture(scope="class", autouse=True)
    def initial_run_and_rename_project_dir(self, project, local_dependency_files):
        initial_project_root = project.project_root
        renamed_project_root = os.path.join(project.project_root.dirname, "renamed_project_dir")

        write_project_files(project.project_root, "local_dependency", local_dependency_files)

        # initial run
        run_dbt(["deps"])
        assert len(run_dbt(["seed"])) == 1
        assert len(run_dbt(["run"])) == 2

        self.rename_project_root(project, renamed_project_root)
        yield
        self.rename_project_root(project, initial_project_root)

    def test_pp_renamed_project_dir_unchanged_project_contents(self, project):
        # partial parse same project in new absolute dir location, using partial_parse.msgpack created in previous dir
        run_dbt(["deps"])
        assert len(run_dbt(["--partial-parse", "seed"])) == 1
        assert len(run_dbt(["--partial-parse", "run"])) == 2

    def test_pp_renamed_project_dir_changed_project_contents(self, project):
        write_file(model_two_sql, project.project_root, "models", "model_two.sql")

        # partial parse changed project in new absolute dir location, using partial_parse.msgpack created in previous dir
        run_dbt(["deps"])
        len(run_dbt(["--partial-parse", "seed"])) == 1
        len(run_dbt(["--partial-parse", "run"])) == 3
