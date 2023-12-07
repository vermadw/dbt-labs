from typing import Optional, Dict, Any
from typing_extensions import Protocol

from dbt.common.clients.jinja import MacroProtocol


class MacroClient(Protocol):
    def find_macro_by_name(
        self, name: str, root_project_name: str, package: Optional[str]
    ) -> Optional[MacroProtocol]:
        raise NotImplementedError("find_macro_by_name not implemented")

    def generate_context_for_macro(
        self, config, macro: MacroProtocol, package: Optional[str]
    ) -> Dict[str, Any]:
        raise NotImplementedError("generate_context_for_macro not implemented")
