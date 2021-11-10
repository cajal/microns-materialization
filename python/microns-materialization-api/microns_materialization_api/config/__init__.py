"""
Configuration package/module for microns-materialization.
"""

from . import adapters
from . import externals

import traceback

try:
    import datajoint as dj
except:
    traceback.print_exc()
    raise ImportError('DataJoint package not found.')

from enum import Enum

from microns_utils import config_utils
    
config_utils.enable_datajoint_flags()

def register_externals(schema_name:str):
    """
    Registers the external stores for a schema_name in this module.
    """
    return config_utils.register_externals(config_mapping[SCHEMAS(schema_name)]["externals"])


def register_adapters(schema_name:str, context=None):
    """
    Imports the adapters for a schema_name into the global namespace.
    """     
    return config_utils.register_adapters(config_mapping[SCHEMAS(schema_name)]["adapters"], context=context)


def create_vm(schema_name:str):
    """
    Creates a virtual module after registering the external stores, and includes the adapter objects in the vm.
    """
    schema = SCHEMAS(schema_name)
    return config_utils.create_vm(schema.value, external_stores=config_mapping[schema]["externals"], adapter_objects=config_mapping[schema]["adapters"])


class SCHEMAS(Enum):
    H01_MATERIALIZATION = "microns_h01_materialization"
    MINNIE65_MATERIALIZATION = "microns_minnie65_materialization"


config_mapping = {
    SCHEMAS.H01_MATERIALIZATION: {
        "externals": externals.h01_materialization,
        "adapters": adapters.h01_materialization_adapter_objects,
    },
    SCHEMAS.MINNIE65_MATERIALIZATION: {
        "externals": externals.minnie65_materialization,
        "adapters": adapters.minnie65_materialization_adapter_objects
    },
    
}
