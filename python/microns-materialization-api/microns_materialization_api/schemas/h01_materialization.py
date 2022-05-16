"""
DataJoint tables for importing h01 from Jeff Lichtman group.
"""
import datajoint as dj
import datajoint_plus as djp

from ..config import h01_materialization_config as config

config.register_externals()
config.register_adapters(context=locals())

schema = dj.schema(config.schema_name, create_schema=True)

schema.spawn_missing_classes()
schema.connection.dependencies.load()