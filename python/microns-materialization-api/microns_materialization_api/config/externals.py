from pathlib import Path
# TODO: place in microns-utils
def make_store_dict(path):
    return {
        'protocol': 'file',
        'location': str(path),
        'stage': str(path)
    }


#h01 materialization
h01_materialization_external_meshes_path = Path() / '/mnt' / 'dj-stor01' / 'microns'/ 'h01' / 'meshes'
h01_materialization = {
    'h01_meshes': make_store_dict(h01_materialization_external_meshes_path),
    
    }



#minnie65_materialization
minnie65_materialization_external_meshes_path = Path() / '/mnt' / 'dj-stor01' / 'microns'/ 'minnie' / 'meshes'
minnie65_materialization = {
    'minnie65_meshes': make_store_dict(minnie65_materialization_external_meshes_path),
    
    }

