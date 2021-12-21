import datajoint.datajoint_plus as djp
from . import h01_materialization

djp.reassign_master_attribute(h01_materialization)
