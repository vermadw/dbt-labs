from typing import Any, Dict, Optional, List, Iterator, Mapping

from dbt.clients.jinja import MacroGenerator, MacroStack
from dbt.contracts.graph.nodes import Macro
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME
from dbt.exceptions import DuplicateMacroNameError, PackageNotFoundForMacroError


class VirtualMacroNamespace(Mapping[str, Any]):
    def __init__(
        self,
        macros_by_name: Mapping[str, List[Macro]],
        ctx: Dict[str, Any],
        root_package: str,
        search_package: str,
        thread_ctx: MacroStack,
        internal_packages: List[str],
        node: Optional[Any] = None,
    ):
        self.macros_by_name = macros_by_name
        self.ctx = ctx
        self.root_package = root_package
        self.search_package = search_package
        self.thread_ctx = thread_ctx
        self.internal_packages = internal_packages
        self.internal_package_names = set(internal_packages)
        self.node = node

    # self.local_namespace
    # self.global_namespace "root package"
    # self.packages
    # { GLOBAL_PROJECT_NAME: self.global_project_namespace }
    # self.global_project_namespace ignoring seemingly broken  wtf?

    def priority(self, macro: Macro):
        if macro.package_name in self.internal_package_names:
            return self.internal_packages.index(macro.package_name)

        if macro.package_name == self.search_package:
            return -3  # highest priority for local package
        elif macro.package_name == self.root_package:
            return -2  # next highest priority for root package
        else:
            return -1  # next priority to non-local, non-root packages.

    def __bool__(self):
        return bool(self.macros_by_name)

    def __iter__(self):
        for k in self.macros_by_name.keys():
            yield k
        yield GLOBAL_PROJECT_NAME

    def __len__(self):
        len(self.macros_by_name) + 1

    def __getitem__(self, key: str):
        if key == GLOBAL_PROJECT_NAME:
            return {
                m.name: MacroGenerator(m, self.ctx, self.node, self.thread_ctx)
                for ml in self.macros_by_name.values()
                for m in ml
                if m.package_name in self.internal_package_names
            }

        candidates = self.macros_by_name.get(key, [])
        if len(candidates) == 0:
            raise KeyError(key)

        candidates.sort(key=self.priority)
        macro = candidates[0]
        return MacroGenerator(macro, self.ctx, self.node, self.thread_ctx)

    def get_from_package(self, package_name: Optional[str], name: str) -> Optional[MacroGenerator]:
        if package_name is None:
            return self.get(name)
        elif package_name == GLOBAL_PROJECT_NAME:
            candidates = self.macros_by_name.get(name, [])
            candidates.sort(key=self.priority)
            internals = [c for c in candidates if c.package_name in self.internal_package_names]

            if len(internals) > 0:
                macro = internals[0]
                return MacroGenerator(macro, self.ctx, self.node, self.thread_ctx)
            return None
        else:
            candidates = self.macros_by_name.get(name, [])
            matches = [c for c in candidates if c.package_name == package_name]
            if len(matches) == 0:
                return None
            elif len(matches) > 1:
                raise DuplicateMacroNameError(matches[0], matches[1], package_name)
            else:
                return MacroGenerator(matches[0], self.ctx, self.node, self.thread_ctx)


class VirtualMacroNamespaceBuilder:
    def __init__(
        self,
        root_package: str,
        search_package: str,
        thread_ctx: MacroStack,
        internal_packages: List[str],
        node: Optional[Any] = None,
    ) -> None:
        self.root_package = root_package
        self.search_package = search_package
        self.thread_ctx = thread_ctx
        self.internal_packages = internal_packages
        self.node = node

    def build_namespace(
        self, macros_by_name: Mapping[str, List[Macro]], ctx: Dict[str, Any]
    ) -> VirtualMacroNamespace:
        return VirtualMacroNamespace(
            macros_by_name,
            ctx,
            root_package=self.root_package,
            search_package=self.search_package,
            thread_ctx=self.thread_ctx,
            internal_packages=self.internal_packages,
            node=self.node,
        )
