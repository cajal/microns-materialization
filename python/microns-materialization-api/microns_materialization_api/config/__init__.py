"""
Configuration package/module for microns-materialization.
"""

import inspect
import traceback
from enum import Enum
from . import adapters
from . import externals
from . import bases
try:
    import datajoint as dj
except:
    traceback.print_exc()
    raise ImportError('DataJoint package not found.')
from microns_utils import config_utils


config_utils.enable_datajoint_flags()


def register_externals(schema_name:str):
    """
    Registers the external stores for a schema_name in this module.
    """
    external_stores = config_mapping[SCHEMAS(schema_name)]["externals"]
    
    if external_stores is not None:
        config_utils.register_externals(external_stores)


def register_adapters(schema_name:str, context=None):
    """
    Imports the adapters for a schema_name into the global namespace.
    """
    adapter_objects = config_mapping[SCHEMAS(schema_name)]["adapters"]
    
    if adapter_objects is not None:
        config_utils.register_adapters(adapter_objects, context=context)


def register_bases(schema_name:str, module):
    """
    Maps base classes to DataJoint tables.
    """
    bases = config_mapping[SCHEMAS(schema_name)]["bases"]

    if bases is not None:
        for base in bases:
            config_utils.register_bases(base, module)
        return module


def create_vm(schema_name:str):
    """
    Creates a virtual module after registering the external stores, adapter objects, DatajointPlus and base classes.
    """
    schema = SCHEMAS(schema_name)
    vm = config_utils._create_vm(schema.value, external_stores=config_mapping[schema]["externals"], adapter_objects=config_mapping[schema]["adapters"])
    config_utils.add_datajoint_plus(vm)
    register_bases(schema_name, vm)
    return vm


class SCHEMAS(Enum):
    H01_MATERIALIZATION = "microns_h01_materialization"
    MINNIE65_MATERIALIZATION = "microns_minnie65_materialization"


config_mapping = {
    SCHEMAS.H01_MATERIALIZATION: {
        "externals": externals.h01_materialization,
        "adapters": adapters.h01_materialization_adapter_objects,
        "bases": None
    },
    SCHEMAS.MINNIE65_MATERIALIZATION: {
        "externals": externals.minnie65_materialization,
        "adapters": adapters.minnie65_materialization_adapter_objects,
        "bases": None
    },
    
}
