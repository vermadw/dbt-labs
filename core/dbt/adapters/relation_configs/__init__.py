from dbt.adapters.relation_configs._materialized_view import (
    MaterializedViewRelationConfig,
)
from dbt.adapters.relation_configs.config_base import (
    RelationConfigBase,
    RelationResults,
)
from dbt.adapters.relation_configs.config_change import (
    RelationConfigChangeAction,
    RelationConfigChange,
)
from dbt.adapters.relation_configs.config_validation import (
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.adapters.relation_configs.factory import RelationConfigFactory
