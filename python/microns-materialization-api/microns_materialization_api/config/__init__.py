"""
Configuration package/module for microns-materialization.
"""
import datajoint_plus as djp
from microns_utils.config_utils import SchemaConfig
from . import adapters
from . import externals

djp.enable_datajoint_flags()

h01_materialization_config = SchemaConfig(
    module_name='h01_materialization',
    schema_name='microns_h01_materialization',
    externals=externals.h01_materialization,
    adapters=adapters.h01_materialization
)

minnie65_materialization_config = SchemaConfig(
    module_name='minnie65_materialization',
    schema_name='microns_minnie65_materialization_v3',
    externals=externals.minnie65_materialization,
    adapters=adapters.minnie65_materialization
)
