"""
Configuration package/module for microns-MATERIALIZATION.
"""

from . import adapters
from . import externals

import traceback

try:
    import datajoint as dj
except:
    traceback.print_exc()
    raise ImportError('DataJoint package not found.')

# Also important to run the dj flags at SOME point during normal datajoint initialization, probably don't want to have this in every config though?

# TODO: place in microns-utils
def enable_datajoint_flags(enable_python_native_blobs=True):
    """
    Enable experimental datajoint features
    
    These flags are required by 0.12.0+ (for now).
    """
    dj.config['enable_python_native_blobs'] = enable_python_native_blobs
    dj.errors._switch_filepath_types(True)
    dj.errors._switch_adapted_types(True)
    
enable_datajoint_flags()

# IMPORTANT: You can organize several schemas' config files (or folders) however you want as long as
# the correct schema configurations are plugged in for the enum variations (obviously).

from enum import Enum

class SCHEMAS(Enum):
    H01_MATERIALIZATION = "microns_h01_materialization"
    MINNIE65_MATERIALIZATION = "microns_minnie65_materialization"



def register_externals(schema_name:str=None,
                       stores_config:dict=None):
    """
    Registers the external stores for a schema in this module.
    """
    
    if stores_config is None and schema_name is None:
        raise Exception("Either stores_config or schema_name must be set")
    
    if schema_name is not None: 
        stores_config = externals_mapping[schema_name]
    
    if 'stores' not in dj.config:
        dj.config['stores'] = stores_config
    else:
        dj.config['stores'].update(stores_config)


def register_adapters(
    schema_name:str=None,
      adapter_objects:dict=None, 
      context=None):
    """
    Imports the adapters for a schema into the global namespace.
    
    This function is probably not necessary, but standardization is nice.
    """
    if adapter_objects is None and schema_name is None:
        raise Exception("Either adapter_objects or schema_name must be set")
    if schema_name is not None: 
        adapter_objects = adapters_mapping[schema_name]
    
    if context is None:
        # if context is missing, use the calling namespace
        import inspect
        frame = inspect.currentframe().f_back
        context = frame.f_locals
        del frame
    
    for name, adapter in adapter_objects.items():
        context[name] = adapter


# Typing annotation hints not strictly necessary. This import is also not necessary if you only specify one type.
from typing import Union


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

adapters_mapping = {
    "microns_h01_materialization":config_mapping[SCHEMAS.H01_MATERIALIZATION]["adapters"],
    "microns_minnie65_materialization":config_mapping[SCHEMAS.MINNIE65_MATERIALIZATION]["adapters"],
}

externals_mapping = {
    "microns_h01_materialization":config_mapping[SCHEMAS.H01_MATERIALIZATION]["externals"],
    "microns_minnie65_materialization":config_mapping[SCHEMAS.MINNIE65_MATERIALIZATION]["externals"],
}


def create_vm(schema_name:str):
    """
    Creates a virtual module after registering the external stores, and includes the adapter objects in the vm.
    """
    
    # Steps that create_vm should take for each schema:
    # 1. Register externals with dj.config
    # 2. Choose which schema's config to load.
    # 3. Load a dict with the relevant adapters into the adapter object field of a virtual module.
    if externals is not None:
        register_externals(schema_name)
    
    return dj.create_virtual_module(schema_name, schema_name, add_objects=adapters_mapping[schema_name])