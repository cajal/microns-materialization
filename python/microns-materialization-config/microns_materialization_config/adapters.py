import datajoint as dj
import numpy as np
import h5py
import os
import trimesh

from collections import namedtuple


class MeshAdapter(dj.AttributeAdapter):
    # Initialize the correct attribute type (allows for use with multiple stores)
    def __init__(self, attribute_type):
        self.attribute_type = attribute_type
        super().__init__()

    attribute_type = '' # this is how the attribute will be declared

    TriangularMesh = namedtuple('TriangularMesh', ['segment_id', 'vertices', 'faces'])
    
    def put(self, filepath):
        # save the filepath to the mesh
        filepath = os.path.abspath(filepath)
        assert os.path.exists(filepath)
        return filepath

    def get(self, filepath):
        # access the h5 file and return a mesh
        assert os.path.exists(filepath)

        with h5py.File(filepath, 'r') as hf:
            vertices = hf['vertices'][()].astype(np.float64)
            faces = hf['faces'][()].reshape(-1, 3).astype(np.uint32)
        
        segment_id = os.path.splitext(os.path.basename(filepath))[0]

        return trimesh.Trimesh(vertices = vertices,faces=faces)


# instantiate for use as a datajoint type
h01_meshes = MeshAdapter('filepath@h01_meshes')
minnie65_meshes = MeshAdapter('filepath@minnie65_meshes')

# also store in one object for ease of use with virtual modules

h01_materialization_adapter_objects = {
    'h01_meshes': h01_meshes,
}

minnie65_materialization_adapter_objects = {
    'minnie65_meshes': minnie65_meshes,
}
