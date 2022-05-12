"""
Externals for DataJoint tables.
"""
from pathlib import Path

import datajoint.datajoint_plus as djp

base_path = Path() / '/mnt' / 'dj-stor01' / 'microns'

#h01 materialization
h01_materialization_external_meshes_path = base_path / 'h01' / 'meshes'

h01_materialization = {
    'h01_meshes': djp.make_store_dict(h01_materialization_external_meshes_path),
}

#minnie65_materialization
minnie65_materialization_external_meshes_path = base_path / 'minnie65' / 'meshes'
minnie65_materialization_external_meshwork_path = base_path / 'minnie65' / 'meshwork'
minnie65_materialization_external_pcg_skeletons_path = base_path / 'minnie65' / 'pcg_skeletons'

minnie65_materialization = {
    'minnie65_meshes': djp.make_store_dict(minnie65_materialization_external_meshes_path),
    'minnie65_meshwork': djp.make_store_dict(minnie65_materialization_external_meshwork_path),
    'minnie65_pcg_skeletons': djp.make_store_dict(minnie65_materialization_external_pcg_skeletons_path),
}
