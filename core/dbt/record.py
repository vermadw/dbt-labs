from types import NoneType
from agate import Table
from collections import defaultdict
from dbt.events.contextvars import get_node_info
import dataclasses
from io import StringIO
import json
import re
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from dbt.contracts.connection import AdapterResponse
from dbt.events.functions import fire_event
from dbt.events.types import Note
from dbt.flags import get_flags
from dbt.version import __version__ as dbt_version


@dataclasses.dataclass
class QueryRecord:
    sql: str
    adapter_response: Optional[AdapterResponse]
    table: Optional[Table]
    node_unique_id: str

    def to_dict(self) -> Any:
        buf = StringIO()
        self.table.to_json(buf)  # type: ignore

        return {
            "sql": self.sql,
            "adapter_response": self.adapter_response.to_dict(),  # type: ignore
            "table": buf.getvalue(),
            "node_unique_id": self.node_unique_id,
        }

    @classmethod
    def from_dict(cls, dct: Mapping) -> "QueryRecord":
        return QueryRecord(
            sql = dct["sql"],
            adapter_response = AdapterResponse.from_dict(dct["adapter_response"]),
            table = Table.from_object(json.loads(dct["table"])),
            node_unique_id=dct["node_unique_id"]
        )


class Record():
    params_cls: type
    result_cls: Optional[type]

    def __init__(self, params, result) -> None:
        self.params = params
        self.result = result

    def to_dict(self) -> Dict[str, Any]:
        return {
            "params": dataclasses.asdict(self.params), #type: ignore
            "result": dataclasses.asdict(self.result) if self.result is not None else None #type: ignore
        }
    
    @classmethod
    def from_dict(cls, dct: Mapping) -> "Record":
        p = cls.params_cls(**dct["params"])
        r = cls.result_cls(**dct["result"]) if cls.result_cls is not None else None
        return cls(params=p, result=r)
    
#
# Record of file load operations
#
     
@dataclasses.dataclass
class LoadFileParams:
    path: str
    strip: bool


@dataclasses.dataclass
class LoadFileResult:
    contents: str


class LoadFileRecord(Record):
    params_cls = LoadFileParams
    result_cls = LoadFileResult


#
# Record of file write operations
#

@dataclasses.dataclass
class WriteFileParams:
    path: str
    contents: str

class WriteFileRecord(Record):
    params_cls = WriteFileParams
    result_cls = None


#
# Record of calls to the directory search function find_matching()
#

@dataclasses.dataclass
class FindMatchingParams:
    root_path: str
    relative_paths_to_search: List[str]
    file_pattern: str
    # ignore_spec: Optional[PathSpec] = None

@dataclasses.dataclass
class FindMatchingResult:
    matches: List[Dict[str, Any]]

class FindMatchingRecord(Record):
    params_cls = FindMatchingParams
    result_cls = FindMatchingResult


class Diff:
    pass


@dataclasses.dataclass
class UnexpectedQueryDiff(Diff):
    sql: str
    node_unique_id: str


@dataclasses.dataclass
class FileWriteDiff(Diff):
    path: str
    recorded_contents: str
    replay_contents: str


@dataclasses.dataclass
class UnexpectedFileWriteDiff(Diff):
    path: str
    contents: str


class Recorder:

    _record_cls_by_name = {
        "LoadFile": LoadFileRecord,
        "WriteFile": WriteFileRecord,
        "FindMatching": FindMatchingRecord,
    }

    _record_name_by_record_type = {
        LoadFileRecord: "LoadFile",
        WriteFileRecord: "WriteFile",
        FindMatchingRecord: "FindMatching",
    }

    _record_name_by_params_type = {
        LoadFileParams: "LoadFile",
        WriteFileParams: "WriteFile",
        FindMatchingParams: "FindMatching",
    }

    def __init__(self) -> None:
        self._queries: MutableMapping[str, List[QueryRecord]] = defaultdict(list)
        self._records_by_type: Dict[str, List[Record]] = {}

    def add_query_record(self, sql: str, response: AdapterResponse, table: Table) -> None:        
        node_info = get_node_info()
        node_unique_id = node_info["unique_id"] if node_info else ""
        query_record = QueryRecord(sql, response, table, node_unique_id)
        self._queries[node_unique_id].append(query_record)

    def pop_query_record(self, node_unique_id: str, sql: str) -> Optional[QueryRecord]:
        node_queries = self._queries[node_unique_id]
        record = node_queries.pop(0) if node_queries else None
        if record and matches(sql, record.sql):
            return record

        return None
    
    def add_record(self, record: Record) -> None:
        rec_type = self._record_name_by_record_type[type(record)]  # type: ignore
        if rec_type not in self._records_by_type:
            self._records_by_type[rec_type] = []
        self._records_by_type[rec_type].append(record)

    def pop_record(self, params: Any) -> Optional[Record]:
        rec_type_name = self._record_name_by_params_type[type(params)]
        records = self._records_by_type[rec_type_name]
        if len(records) > 0 and records[0].params == params:
            r = records[0]
            records.pop(0)
            return r
        else:
            return None
        
    def add_load_file_record(self, path: str, strip: bool, contents: str) -> None:
        self.add_record(
            LoadFileRecord(
                params=LoadFileParams(path, strip),
                result=LoadFileResult(contents)
            )
        )

    def add_write_file_record(self, path: str, contents: str) -> None:
        self.add_record(
            WriteFileRecord(
                params=WriteFileParams(path, contents),
                result=None,
            )
        )

    def add_find_matching_record(self, root_path: str, relative_paths_to_search: List[str], file_pattern: str, matches: List[Dict[str, Any]]):
        self.add_record(
            FindMatchingRecord(
                params=FindMatchingParams(root_path, relative_paths_to_search, file_pattern),
                result=FindMatchingResult(matches),
            )
        )

    def write(self, file_name) -> None:
        with open(file_name, "w") as file:
            json.dump(self.to_dict(), file)

    def to_dict(self) -> Dict:
        dct: Dict[str, Any]= {}
        dct["queries"] = []
        for query_list in self._queries.values():
            for query in query_list:
                dct["queries"].append(query.to_dict())

        for record_type in self._records_by_type:
            record_list = [r.to_dict() for r in self._records_by_type[record_type]]
            dct[record_type] = record_list

        return dct

    @classmethod
    def load(self, file_name: str) -> "Recorder":
        with open(file_name) as file:
            loaded_dct = json.load(file)

        recorder = Recorder()
        for query_dct in loaded_dct["queries"]:
            query = QueryRecord.from_dict(query_dct)
            recorder._queries[query.node_unique_id].append(query)

        for record_type_name in loaded_dct:
            if record_type_name == "queries":
                continue
            record_cls = self._record_cls_by_name[record_type_name]
            rec_list = []
            for record_dct in loaded_dct[record_type_name]:
                rec_list.append(record_cls.from_dict(record_dct))  # type: ignore
            recorder._records_by_type[record_type_name] = rec_list
                
        return recorder
       

class Replayer:
    def __init__(self, recording: Recorder) -> None:
        self.recording = recording
        self._diffs: MutableMapping[str, List[Diff]] = defaultdict(list)
        self._misc_diffs: List[Diff] = []

    def expect_query_record(self, sql: str) -> QueryRecord:
        node_info = get_node_info()
        node_unique_id = node_info["unique_id"] if node_info else ""        

        record = self.recording.pop_query_record(node_unique_id, sql)

        if record is None:
            default_adapter_response = AdapterResponse.from_dict(
                {"_message": "", "code": "SUCCESS", "rows_affected": 0, "query_id": ""}
            )
            default_table_response = Table.from_object(json.loads("{}"))#
            self._diffs[node_unique_id].append(UnexpectedQueryDiff(sql, node_unique_id))
            return QueryRecord(
                sql="",
                adapter_response=default_adapter_response,
                table=default_table_response,
                node_unique_id=node_unique_id,
            )

        return record
    
    def expect_load_file_record(self, path: str, trim: bool) -> str:

        record = self.recording.pop_record(LoadFileParams(path, trim))
        
        if record is None:
            raise Exception()

        return record.result.contents

    def expect_write_file_record(self, path: str, contents: str) -> None:
        record = self.recording.pop_record(WriteFileParams(path, contents))

        if record is None:
            self._misc_diffs.append(UnexpectedFileWriteDiff(path, contents))
        else:
            self._misc_diffs.append(FileWriteDiff(path, record.result.contents, contents))

    def expect_find_matching_record(self, root_path: str, relative_paths_to_search: List[str], file_pattern: str) -> List[Dict[str, Any]]:
        record = self.recording.pop_record(FindMatchingParams(root_path, relative_paths_to_search, file_pattern))

        if record is None:
            raise Exception()

        return record.result.matches

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


def clean_up_sql(sql):
    sql = re.sub(r"--.*?\n", "", sql)  # Remove single-line comments (--)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)  # Remove multi-line comments (/* */)
    return sql.replace(" ", "").replace("\n", "")


def matches(sql1, sql2):
    return clean_up_sql(sql1) == clean_up_sql(sql2)


CURRENT_REPLAYER: Optional[Replayer] = None


def load_baseline_recording():
    global CURRENT_REPLAYER
    CURRENT_REPLAYER = Replayer(Recorder.load(get_flags().EXECUTION_RECORD_PATH))



CURRENT_RECORDER: Recorder = Recorder()


def get_recorder() -> Recorder:
    return CURRENT_RECORDER


def get_replayer() -> Replayer:

    if CURRENT_REPLAYER is None:
        raise Exception()

    return CURRENT_REPLAYER


def write_recording():
    file_name = f"{get_flags().TARGET_PATH or './target'}/executed-{dbt_version}.json"
    fire_event(Note(msg=f"Writing execution recoring to {file_name}"))
    CURRENT_RECORDER.write(file_name)


def write_recording_diffs():
    pass#
