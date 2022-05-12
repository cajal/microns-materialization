"""
Adapters for DataJoint tables.
"""

import json

import h5py
import trimesh
from meshparty import meshwork, skeleton
from microns_utils.adapter_utils import FilePathAdapter, adapt_mesh_hdf5


class TrimeshAdapter(FilePathAdapter):
    def get(self, filepath):
        filepath = super().get(filepath)
        mesh = adapt_mesh_hdf5(filepath, parse_filepath_stem=False, return_type='namedtuple')
        return trimesh.Trimesh(vertices=mesh.vertices, faces=mesh.faces)


class MeshworkAdapter(FilePathAdapter):
    def get(self, filepath):
        filepath = super().get(filepath)
        return meshwork.load_meshwork(filepath)


class PCGSkelAdapter(FilePathAdapter):
    def get(self, filepath):
        filepath = super().get(filepath)
        with h5py.File(filepath, 'r') as f:
            vertices = f['vertices'][()]
            edges = f['edges'][()]
            mesh_to_skel_map = f['mesh_to_skel_map'][()]
            root = f['root'][()]
            meta = json.loads(f['meta'][()])
            skel = skeleton.Skeleton(vertices=vertices, edges=edges, mesh_to_skel_map=mesh_to_skel_map, root=root, meta=meta)
        return skel

# M65
minnie65_meshes = TrimeshAdapter('filepath@minnie65_meshes')
minnie65_meshwork = MeshworkAdapter('filepath@minnie65_meshwork')
minnie65_pcg_skeletons = PCGSkelAdapter('filepath@minnie65_pcg_skeletons')

minnie65_materialization = {
    'minnie65_meshes': minnie65_meshes,
    'minnie65_meshwork': minnie65_meshwork,
    'minnie65_pcg_skeletons': minnie65_pcg_skeletons
}

# H01
h01_meshes = TrimeshAdapter('filepath@h01_meshes')

h01_materialization = {
    'h01_meshes': h01_meshes,
}
