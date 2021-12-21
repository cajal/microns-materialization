"""
DataJoint tables for importing h01 from Jeff Lichtman group.
"""
import datajoint as dj
import datajoint.datajoint_plus as djp

from ..config import h01_materialization_config

h01_materialization_config.register_externals()
h01_materialization_config.register_adapters(context=locals())

schema = dj.schema(h01_materialization_config.schema_name, create_schema=True)
