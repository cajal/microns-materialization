from microns_materialization_api import config
from . import h01_materialization

config.register_bases(config.SCHEMAS.H01_MATERIALIZATION, h01_materialization)