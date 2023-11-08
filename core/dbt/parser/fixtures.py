from dbt.context.context_config import ContextConfig
from dbt.contracts.graph.nodes import FixtureNode
from dbt.node_types import NodeType
from dbt.parser.base import SimpleSQLParser
from dbt.parser.search import FileBlock


class FixtureParser(SimpleSQLParser[FixtureNode]):
    def parse_from_dict(self, dct, validate=True) -> FixtureNode:
        # fixtures need the root_path because the contents are not loaded
        dct["root_path"] = self.project.project_root
        if "language" in dct:  # TODO: ?? this was there for seeds
            del dct["language"]
        # raw_code is not currently used, but it might be in the future
        if validate:
            FixtureNode.validate(dct)
        return FixtureNode.from_dict(dct)

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Fixture  # TODO: ??

    @classmethod
    def get_compiled_path(cls, block: FileBlock):
        return block.path.relative_path

    def render_with_context(self, parsed_node: FixtureNode, config: ContextConfig) -> None:
        """Fixtures don't need to do any rendering."""
