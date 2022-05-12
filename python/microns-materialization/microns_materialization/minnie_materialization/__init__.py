import datajoint.datajoint_plus as djp
from . import minnie65_materialization

djp.reassign_master_attribute(minnie65_materialization)