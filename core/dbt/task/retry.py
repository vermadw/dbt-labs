from pathlib import Path

from dbt.cli.flags import Flags
from dbt.flags import set_flags, get_flags
from dbt.adapters.factory import register_adapter
from dbt.cli.types import Command as CliCommand
from dbt.config import RuntimeConfig
from dbt.contracts.results import NodeStatus
from dbt.contracts.state import PreviousState
from dbt.common.exceptions import DbtRuntimeError
from dbt.graph import GraphQueue
from dbt.task.base import ConfiguredTask
from dbt.task.build import BuildTask
from dbt.task.clone import CloneTask
from dbt.task.compile import CompileTask
from dbt.task.docs.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.run_operation import RunOperationTask
from dbt.task.seed import SeedTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask
from dbt.parser.manifest import ManifestLoader, write_manifest
from dbt.plugins import get_plugin_manager

RETRYABLE_STATUSES = {NodeStatus.Error, NodeStatus.Fail, NodeStatus.Skipped, NodeStatus.RuntimeErr}
OVERRIDE_PARENT_FLAGS = {
    "log_path",
    "output_path",
    "profiles_dir",
    "profiles_dir_exists_false",
    "project_dir",
    "defer_state",
    "deprecated_state",
    "target_path",
    "vars",
}

TASK_DICT = {
    "build": BuildTask,
    "compile": CompileTask,
    "clone": CloneTask,
    "generate": GenerateTask,
    "seed": SeedTask,
    "snapshot": SnapshotTask,
    "test": TestTask,
    "run": RunTask,
    "run-operation": RunOperationTask,
}

CMD_DICT = {
    "build": CliCommand.BUILD,
    "compile": CliCommand.COMPILE,
    "clone": CliCommand.CLONE,
    "generate": CliCommand.DOCS_GENERATE,
    "seed": CliCommand.SEED,
    "snapshot": CliCommand.SNAPSHOT,
    "test": CliCommand.TEST,
    "run": CliCommand.RUN,
    "run-operation": CliCommand.RUN_OPERATION,
}


class RetryTask(ConfiguredTask):
    def __init__(self, args, config, manifest) -> None:
        state_path = args.state or config.target_path
        self.previous_state = PreviousState(
            state_path=Path(state_path),
            target_path=Path(config.target_path),
            project_root=Path(config.project_root),
        )

        if not self.previous_state.results:
            raise DbtRuntimeError(
                f"Could not find previous run in '{state_path}' target directory"
            )
        self.previous_args = self.previous_state.results.args
        self.previous_command_name = self.previous_args.get("which")

        if args.warn_error:
            RETRYABLE_STATUSES.add(NodeStatus.Warn)

        cli_command = CMD_DICT.get(self.previous_command_name)  # type: ignore
        # Remove these args when their default values are present, otherwise they'll raise an exception
        args_to_remove = {
            "show": lambda x: True,
            "resource_types": lambda x: x == [],
            "warn_error_options": lambda x: x == {"exclude": [], "include": []},
        }

        for k, v in args_to_remove.items():
            if k in self.previous_args and v(self.previous_args[k]):
                del self.previous_args[k]

        previous_args = {
            k: v for k, v in self.previous_args.items() if k not in OVERRIDE_PARENT_FLAGS
        }
        current_args = {k: v for k, v in args.__dict__.items() if k in OVERRIDE_PARENT_FLAGS}
        combined_args = {**previous_args, **current_args}
        retry_flags = Flags.from_dict(cli_command, combined_args)  # type: ignore
        set_flags(retry_flags)
        retry_config = RuntimeConfig.from_args(args=retry_flags)
        # This logic is being called in requires.py, probably best to refactor it to a function
        register_adapter(retry_config)

        manifest = ManifestLoader.get_full_manifest(
            retry_config,
            write_perf_info=False,
        )

        if retry_flags.write_json:  # type: ignore
            write_manifest(manifest, retry_config.project_target_path)
            pm = get_plugin_manager(retry_config.project_name)
            plugin_artifacts = pm.get_manifest_artifacts(manifest)
            for path, plugin_artifact in plugin_artifacts.items():
                plugin_artifact.write(path)
        super().__init__(args, retry_config, manifest)
        self.task_class = TASK_DICT.get(self.previous_command_name)  # type: ignore
        self.retry_flags = retry_flags

    def run(self):
        unique_ids = set(
            [
                result.unique_id
                for result in self.previous_state.results.results
                if result.status in RETRYABLE_STATUSES
            ]
        )

        class TaskWrapper(self.task_class):
            def get_graph_queue(self):
                new_graph = self.graph.get_subset_graph(unique_ids)
                return GraphQueue(
                    new_graph.graph,
                    self.manifest,
                    unique_ids,
                )

        task = TaskWrapper(
            get_flags(),
            self.config,
            self.manifest,
        )

        return_value = task.run()
        return return_value

    def interpret_results(self, *args, **kwargs):
        return self.task_class.interpret_results(*args, **kwargs)
