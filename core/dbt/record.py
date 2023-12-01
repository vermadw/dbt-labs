from agate import Table
from collections import defaultdict
from contextvars import ContextVar
import dataclasses
from io import StringIO
import json
import re
from typing import Dict, List, MutableMapping, Optional

from dbt.contracts.connection import AdapterResponse
from dbt.flags import get_flags
from dbt.version import __version__ as dbt_version


current_node = ContextVar("current_node", default="")  # type: ignore


@dataclasses.dataclass
class QueryRecord:
    sql: str
    adapter_response: Optional[AdapterResponse]
    table: Optional[Table]
    node_unique_id: str


@dataclasses.dataclass
class LoadFileContentsRecord:
    path: str
    strip: bool
    returned_contents: str


class Diff:
    pass


@dataclasses.dataclass
class UnexpectedQueryDiff(Diff):
    sql: str
    node_unique_id: str


class Recording:
    def __init__(self) -> None:
        self._queries: MutableMapping[str, List[QueryRecord]] = defaultdict(list)

    def add_query_record(self, query_record: QueryRecord) -> None:
        self._queries[query_record.node_unique_id].append(query_record)

    def pop_query_record(self, node_unique_id: str, sql: str) -> Optional[QueryRecord]:
        if node_unique_id not in self._queries:
            return None

        prev = self._queries[node_unique_id].pop(0)
        if matches(sql, prev.sql):
            return prev

        return None

    def write(self) -> None:
        out_file_name = f"{get_flags().TARGET_PATH or './target'}/executed-{dbt_version}.json"
        with open(out_file_name, "w") as out_file:
            json.dump(self._queries, out_file)

    def load(self) -> None:
        pass


class RecordingComparator:
    def __init__(self, baseline: Recording) -> None:
        self.baseline = baseline
        self._diffs: MutableMapping[str, List[Diff]] = defaultdict(list)

    def get_query_record(self, node_unique_id: str, sql: str) -> QueryRecord:
        prev = self.baseline.pop_query_record(node_unique_id, sql)

        if prev is None:
            default_adapter_response = AdapterResponse.from_dict(
                {"_message": "", "code": "SUCCESS", "rows_affected": 0, "query_id": ""}
            )
            default_table_response = Table.from_object(json.loads("{}"))  #
            self._diffs[node_unique_id].append(UnexpectedQueryDiff(sql, node_unique_id))
            return QueryRecord(
                sql="",
                adapter_response=default_adapter_response,
                table=default_table_response,
                node_unique_id=node_unique_id,
            )

        return prev

    def write_diffs(self) -> None:
        json.dump(
            self._diffs,
            open(f"{get_flags().TARGET_PATH or './target'}/diffs-{dbt_version}.json", "w"),
        )
        self.print_diffs()

    def end(self):
        """End the comparison between the current recording and the baseline"""
        pass

    def print_diffs(self) -> None:
        print(repr(self._diffs))


def cleanup_sql(sql):
    # Remove single-line comments (--)
    sql = re.sub(r"--.*?\n", "", sql)
    # Remove multi-line comments (/* */)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql.replace(" ", "").replace("\n", "")


def matches(sql1, sql2):
    return cleanup_sql(sql1) == cleanup_sql(sql2)


BASELINE_RECORDING: Optional["Recording"] = None


def load_baseline_recording():
    print("REC: Loading baseline")
    global baseline_recording
    baseline_recording = Recording.load(get_flags().EXECUTION_RECORD_PATH)


CURRENT_RECORDING: Recording = Recording()


def get_recorder() -> Recording:
    return CURRENT_RECORDING


def get_comparer() -> RecordingComparator:
    pass


def write_recording():
    print("REC: writing recording")
    CURRENT_RECORDING.write()


def write_recording_diffs():
    pass  #
