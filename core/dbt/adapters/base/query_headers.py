from threading import local
from typing import Optional, Callable, Dict, Any

from dbt.adapters.clients.jinja import QueryStringGenerator

from dbt.adapters.contracts.connection import AdapterRequiredConfig, QueryComment
from dbt.contracts.graph.nodes import ResultNode
from dbt.common.context import basic_functions
from dbt.common.exceptions import DbtRuntimeError


class NodeWrapper:
    def __init__(self, node) -> None:
        self._inner_node = node

    def __getattr__(self, name):
        return getattr(self._inner_node, name, "")


class _QueryComment(local):
    """A thread-local class storing thread-specific state information for
    connection management, namely:
        - the current thread's query comment.
        - a source_name indicating what set the current thread's query comment
    """

    def __init__(self, initial) -> None:
        self.query_comment: Optional[str] = initial
        self.append: bool = False

    def add(self, sql: str) -> str:
        if not self.query_comment:
            return sql

        if self.append:
            # replace last ';' with '<comment>;'
            sql = sql.rstrip()
            if sql[-1] == ";":
                sql = sql[:-1]
                return "{}\n/* {} */;".format(sql, self.query_comment.strip())

            return "{}\n/* {} */".format(sql, self.query_comment.strip())

        return "/* {} */\n{}".format(self.query_comment.strip(), sql)

    def set(self, comment: Optional[str], append: bool):
        if isinstance(comment, str) and "*/" in comment:
            # tell the user "no" so they don't hurt themselves by writing
            # garbage
            raise DbtRuntimeError(f'query comment contains illegal value "*/": {comment}')
        self.query_comment = comment
        self.append = append


QueryStringFunc = Callable[[str, Optional[NodeWrapper]], str]


class DefaultQueryHeaderContext:
    def __init__(self, config: AdapterRequiredConfig):
        self.config = config

    def to_dict(self) -> Dict[str, Any]:
        return {
            "target": self.config.to_target_dict(),
            "tojson": basic_functions.tojson,
            "return": basic_functions._return,
            "dbt_version": "unknown",
        }


class MacroQueryStringSetter:
    def __init__(
        self, config: AdapterRequiredConfig, query_header_context: Optional[Dict[str, Any]] = None
    ) -> None:
        self.config = config
        # Ensure query_header_context has at least DefaultQueryHeaderContext attributes
        self._query_header_context: Dict[str, Any] = DefaultQueryHeaderContext(config).to_dict()
        if query_header_context:
            self._query_header_context.update(query_header_context)

        comment_macro = self._get_comment_macro()
        self.generator: QueryStringFunc = lambda name, model: ""
        # if the comment value was None or the empty string, just skip it
        if comment_macro:
            assert isinstance(comment_macro, str)
            macro = "\n".join(
                (
                    "{%- macro query_comment_macro(connection_name, node) -%}",
                    comment_macro,
                    "{% endmacro %}",
                )
            )
            ctx = self._get_context()
            self.generator = QueryStringGenerator(macro, ctx)
        self.comment = _QueryComment(None)
        self.reset()

    def _get_comment_macro(self) -> Optional[str]:
        return self.config.query_comment.comment

    def _get_context(self) -> Dict[str, Any]:
        return self._query_header_context

    def add(self, sql: str) -> str:
        return self.comment.add(sql)

    def reset(self):
        self.set("master", None)

    def set(self, name: str, node: Optional[ResultNode]):
        wrapped: Optional[NodeWrapper] = None
        if node is not None:
            wrapped = NodeWrapper(node)
        comment_str = self.generator(name, wrapped)

        append = False
        if isinstance(self.config.query_comment, QueryComment):
            append = self.config.query_comment.append
        self.comment.set(comment_str, append)
