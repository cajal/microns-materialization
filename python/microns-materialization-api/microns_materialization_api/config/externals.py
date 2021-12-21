"""
Externals for DataJoint tables.
"""
import datajoint.datajoint_plus as djp
from pathlib import Path

base_path = Path() / '/mnt' / 'dj-stor01' / 'microns'

#h01 materialization
h01_materialization_external_meshes_path = base_path / 'h01' / 'meshes'
h01_materialization = {
    'h01_meshes': djp.make_store_dict(h01_materialization_external_meshes_path),
}

#minnie65_materialization
minnie65_materialization_external_meshes_path = base_path / 'minnie' / 'meshes'
minnie65_materialization = {
    'minnie65_meshes': djp.make_store_dict(minnie65_materialization_external_meshes_path),
}
