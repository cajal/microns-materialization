"""
DataJoint tables for importing minnie65 from CAVE.
"""
import datajoint as dj
import datajoint_plus as djp
from microns_utils.misc_utils import classproperty

from ..config import minnie65_materialization_config as config

config.register_externals()
config.register_adapters(context=locals())

schema = djp.schema(config.schema_name, create_schema=True)


@schema
class ImportMethod(djp.Lookup):
    hash_name = 'import_method'
    definition = """
    import_method : varchar(8) # import method hash
    """

    class MaterializationVer(djp.Part):
        enable_hashing = True
        hash_name = 'import_method'
        hashed_attrs = ['caveclient_version', 'datastack', 'ver']
        definition = """
        -> master
        ---
        caveclient_version: varchar(48) # version of caveclient installed when method was created
        datastack: varchar(250) # name of datastack
        ver: smallint # client materialization version
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """
     
    class NucleusSegment(djp.Part):
        enable_hashing = True
        hash_name = 'import_method'
        hashed_attrs = 'caveclient_version', 'datastack', 'ver'
        definition = """
        -> master
        ---
        caveclient_version: varchar(48) # version of caveclient installed when method was created
        datastack: varchar(250) # name of datastack
        ver: smallint # client materialization version
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """
        
    class MeshPartyMesh(djp.Part):
        enable_hashing = True
        hash_name = 'import_method'
        hashed_attrs = 'meshparty_version', 'caveclient_version', 'datastack', 'cloudvolume_version', 'cloudvolume_path', 'download_meshes_kwargs', 'target_dir'
        definition = """
        -> master
        ---
        description : varchar(1000) # details
        meshparty_version: varchar(48) # version of meshparty installed when method was created
        caveclient_version: varchar(48) # version of caveclient installed when method was created
        datastack: varchar(250) # name of datastack
        cloudvolume_version: varchar(48) # version of cloudvolume installed when method was created
        cloudvolume_path: varchar(250) # cloudvolume path used to download meshes
        download_meshes_kwargs: varchar(1000) # JSON array passed to meshparty.trimesh_io.download_meshes. Note: use json.loads to recover dict.
        target_dir: varchar(1000) # target directory for mesh files
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

    class MeshPartyMesh2(djp.Part):
        enable_hashing = True
        hash_name = 'import_method'
        hashed_attrs = 'meshparty_version', 'caveclient_version', 'datastack', 'ver', 'cloudvolume_version', 'cloudvolume_path', 'download_meshes_kwargs', 'target_dir'
        definition = """
        -> master
        ---
        description : varchar(1000) # details
        meshparty_version: varchar(48) # version of meshparty installed when method was created
        caveclient_version: varchar(48) # version of caveclient installed when method was created
        datastack: varchar(250) # name of datastack
        ver: smallint # client materialization version
        cloudvolume_version: varchar(48) # version of cloudvolume installed when method was created
        cloudvolume_path: varchar(250) # cloudvolume path used to download meshes
        download_meshes_kwargs: varchar(1000) # JSON array passed to meshparty.trimesh_io.download_meshes. Note: use json.loads to recover dict.
        target_dir: varchar(1000) # target directory for mesh files
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

    class Synapse(djp.Part):
        enable_hashing = True
        hash_name = 'import_method'
        hashed_attrs = 'caveclient_version', 'datastack'
        definition = """
        -> master
        ---
        caveclient_version: varchar(48) # version of caveclient installed when method was created
        datastack: varchar(250) # name of datastack
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """
    
    class Synapse2(djp.Part):
        enable_hashing = True
        hash_name = 'import_method'
        hashed_attrs = 'caveclient_version', 'datastack', 'ver'
        definition = """
        -> master
        ---
        caveclient_version: varchar(48) # version of caveclient installed when method was created
        datastack: varchar(250) # name of datastack
        ver: smallint # client materialization version
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

    class PCGMeshwork(djp.Part):
        enable_hashing = True
        hash_name = 'import_method'
        hashed_attrs = 'meshparty_version', 'pcg_skel_version', 'caveclient_version', 'datastack', 'ver', 'synapse_table', 'nucleus_table', 'cloudvolume_version', 'cloudvolume_path', 'pcg_meshwork_params', 'target_dir'
        definition = """
        -> master
        ---
        description=NULL : varchar(1000) # details
        meshparty_version: varchar(48) # version of meshparty installed when method was created
        pcg_skel_version: varchar(48) # version of pcg_skel installed when method was created
        caveclient_version: varchar(48) # version of caveclient installed when method was created
        datastack: varchar(250) # name of datastack
        ver: smallint # client materialization version
        synapse_table : varchar(250) # synapse table in CAVEclient annotation service
        nucleus_table : varchar(250) # nucleus table in CAVEclient annotation service
        cloudvolume_version: varchar(48) # version of cloudvolume installed when method was created
        cloudvolume_path: varchar(250) # cloudvolume path
        pcg_meshwork_params: varchar(1000) # JSON array passed to pcg_skel.pcg_meshwork. Note: use json.loads to recover dict.
        target_dir: varchar(1000) # target directory for file
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

    class PCGSkeleton(djp.Part):
        enable_hashing = True
        hash_name = 'import_method'
        hashed_attrs = 'meshparty_version', 'pcg_skel_version', 'caveclient_version', 'datastack', 'ver', 'nucleus_table', 'cloudvolume_version', 'cloudvolume_path', 'pcg_skel_params', 'target_dir'
        definition = """
        -> master
        ---
        description=NULL : varchar(1000) # details
        meshparty_version: varchar(48) # version of meshparty installed when method was created
        pcg_skel_version: varchar(48) # version of pcg_skel installed when method was created
        caveclient_version: varchar(48) # version of caveclient installed when method was created
        datastack: varchar(250) # name of datastack
        ver: smallint # client materialization version
        nucleus_table : varchar(250) # nucleus table in CAVEclient annotation service
        cloudvolume_version: varchar(48) # version of cloudvolume installed when method was created
        cloudvolume_path: varchar(250) # cloudvolume path
        pcg_skel_params: varchar(1000) # JSON array passed to pcg_skel.pcg_skel_params. Note: use json.loads to recover dict.
        target_dir: varchar(1000) # target directory for file
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """


@schema
class Materialization(djp.Lookup):
    definition = """
    ver : decimal(6,2) # materialization version
    """

    class Info(djp.Part):
        store = True
        definition = """
        -> master
        ---
        datastack : varchar(48) # datastack name from CAVE metadata
        id=NULL : int # materialization ID from CAVE metadata
        valid : tinyint # 1 if valid
        expires_on=NULL : varchar(32) # date materialization expires from CAVE client (stored as varchar in SQL)
        time_stamp : varchar(32) # timestamp materialization was generated (stored as varchar in SQL)
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

        @classproperty
        def with_timestamps(cls):
            return cls.proj(..., expires_on='str_to_date(expires_on, "%%Y-%%m-%%d %%H:%%i:%%s")', time_stamp='str_to_date(time_stamp, "%%Y-%%m-%%d %%H:%%i:%%s")')

    class Checkpoint(djp.Part):
        definition = """
        name : varchar(48) # name of checkpoint
        ---
        -> master
        description=NULL : varchar(450) # details
        """

    class MatV1(djp.Part, dj.Computed):
        maker = True
        definition = """
        -> master
        -> dj.create_virtual_module('mat_v1', 'microns_minnie65_materialization').Materialization
        -> Materialization.Info
        ---
        valid=1              : tinyint                      # marks whether the materialization is valid. Defaults to 1.
        description=null     : varchar(256)                 # description of the materialization and source
        timestamp : timestamp             # marks the time at which the materialization service was started
        """
    
    class CAVE(djp.Part, dj.Computed):
        maker = True
        definition = """
        -> master
        -> ImportMethod
        -> Materialization.Info
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """

    @classproperty
    def latest(cls):
        return cls.aggr_max('ver')
    
    @classproperty
    def available(cls):
        return cls & (cls.Info.with_timestamps.proj(diff='datediff(expires_on, current_timestamp)') & 'diff > 0')

    @classproperty
    def expired(cls):
        return cls & (cls.Info.with_timestamps.proj(diff='datediff(expires_on, current_timestamp)') & 'diff < 0')

    @classproperty
    def long_term_support(cls):
        return cls & (cls.Info.with_timestamps.proj(diff='datediff(expires_on, time_stamp)') & 'diff >= 30')

@schema
class Nucleus(djp.Lookup):
    definition = """
    nucleus_id : int unsigned # id of segmented nucleus.
    """
    
    class Info(djp.Part):
        definition = """
        -> Materialization
        -> master
        segment_id : bigint unsigned # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
        ---
        nucleus_x            : int unsigned                 # x coordinate of nucleus centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
        nucleus_y            : int unsigned                 # y coordinate of nucleus centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
        nucleus_z            : int unsigned                 # z coordinate of nucleus centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
        supervoxel_id        : bigint unsigned              # id of the supervoxel under the nucleus centroid. Equivalent to Allen: 'pt_supervoxel_id'.
        volume=null          : float                        # volume of the nucleus in um^3
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """
    
    class MatV1(djp.Part):
        definition = """
        -> master
        -> dj.create_virtual_module('mat_v1', 'microns_minnie65_materialization').Nucleus.Info
        -> master.Info
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """
    
    class CAVE(djp.Part, dj.Computed):
        definition = """
        -> master
        -> ImportMethod
        -> master.Info
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """



@schema
class Segment(djp.Lookup):
    definition = """
    segment_id : bigint unsigned # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
    """
    
    class MatV1(djp.Part):
        definition = """
        -> master
        -> Nucleus.MatV1
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """
    
    class Nucleus(djp.Part, dj.Computed):
        definition = """
        -> master
        -> Nucleus.Info
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """


@schema
class Exclusion(djp.Lookup):
    enable_hashing = True
    hash_name = 'exclusion_hash'
    hashed_attrs = ['reason']
    definition = f"""
    {hash_name} : varchar(6) 
    ---
    reason : varchar(48) # reason for exclusion
    """

    contents = [
        {'reason': 'no data'},
        {'reason': 'no synapse data'}
    ]


@schema
class Synapse(djp.Lookup):
    definition = """
    synapse_id           : bigint unsigned              # synapse index within the segmentation
    """
    
    class Info(djp.Part):
        definition = """
        # Synapses from the table 'synapses_pni_2'
        -> Segment.proj(primary_seg_id='segment_id')
        secondary_seg_id                              : bigint unsigned              # id of the segment that is synaptically paired to primary_segment_id.
        -> master
        ---
        prepost                                       : varchar(16)                  # whether the primary_seg_id is "presyn" or "postsyn"
        synapse_x                                     : int unsigned                 # x coordinate of synapse centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm). From Allen 'ctr_pt_position'.
        synapse_y                                     : int unsigned                 # y coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm). From Allen 'ctr_pt_position'.
        synapse_z                                     : int unsigned                 # z coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm). From Allen 'ctr_pt_position'.
        synapse_size                                  : int unsigned                 # (EM voxels) scaled by (4x4x40)
        """
    
    class MatV1(djp.Part):
        definition = """
        -> master.Info
        -> dj.create_virtual_module('mat_v1', 'microns_minnie65_materialization').Synapse
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """
    
    class SegmentExclude(djp.Part):
        definition = """
        -> Segment.proj(primary_seg_id='segment_id')
        -> master
        -> Exclusion
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """

    class CAVE(djp.Part, dj.Computed):
        definition = """
        -> master.Info
        -> ImportMethod
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """


@schema
class Mesh(djp.Lookup):
    hash_name = 'mesh_id'
    definition = """
    mesh_id : varchar(12) # unique identifier of a mesh
    """
    
    class Object(djp.Part):
        definition = """
        -> master
        ---
        n_vertices          : int unsigned        # number of vertices
        n_faces             : int unsigned        # number of faces
        mesh                : <minnie65_meshes>   # in-place path to the hdf5 mesh file
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """
    
    class MeshParty(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'mesh_id'
        hashed_attrs = 'segment_id', 'import_method', 'ts_computed'
        definition = """
        -> master
        -> master.Object
        -> Segment
        -> ImportMethod
        ts_computed : varchar(128) # timestamp (varchar) that mesh was downloaded/ computed
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """


@schema
class Meshwork(djp.Lookup):
    hash_name = 'meshwork_id'
    definition = """
    meshwork_id : varchar(12) # unique identifier of a meshwork object
    """
    contents = [[0]] # default id for rows lacking object

    class PCGMeshwork(djp.Part):
        definition = """
        -> master
        ---
        meshwork_obj : <minnie65_meshwork> # in-place path to the hdf5 file
        """

    class PCGMeshworkExclude(djp.Part):
        definition = """
        -> Segment
        -> Exclusion
        -> master
        ts_computed : varchar(128) # timestamp (varchar) that row was excluded
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp # timestamp inserted
        """

    class PCGMeshworkMaker(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'meshwork_id'
        hashed_attrs = 'segment_id', 'import_method', 'ts_computed'
        definition = """
        -> master.PCGMeshwork
        -> Segment
        -> ImportMethod
        ts_computed : varchar(128) # timestamp (varchar) that row was downloaded/ computed
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """


@schema
class Skeleton(djp.Lookup):
    hash_name = 'skeleton_id'
    definition = """
    skeleton_id : varchar(12) # unique identifier of a skeleton
    """
    contents = [[0]] # default id for rows lacking object

    class PCGSkeleton(djp.Part):
        definition = """
        -> master
        ---
        skeleton_obj : <minnie65_pcg_skeletons>   # in-place path to the hdf5 file
        """

    class PCGSkeletonMaker(djp.Part, dj.Computed):
        enable_hashing = True
        hash_name = 'skeleton_id'
        hashed_attrs = 'segment_id', 'import_method', 'ts_computed'
        definition = """
        -> master.PCGSkeleton
        -> Segment
        -> ImportMethod
        ts_computed : varchar(128) # timestamp (varchar) that row was downloaded/ computed
        ---
        ts_inserted=CURRENT_TIMESTAMP : timestamp
        """

@schema
class Queue(djp.Lookup):
    hash_name = 'queue_id'
    definition = """
    queue_id : varchar(18) # id of queue entry
    ---
    ts_inserted=CURRENT_TIMESTAMP : timestamp
    """

    class PCGSkeleton(djp.Part):
        enable_hashing = True
        hash_name = 'queue_id'
        hashed_attrs = 'segment_id', 'import_method'
        hash_group = True
        
        definition = """
        -> Segment
        -> ImportMethod
        -> master
        """
        
        @classmethod
        def add_rows(cls, rows):
            cls.insert(rows, insert_to_master=True, ignore_extra_fields=True)
    
    class PCGMeshwork(djp.Part):
        enable_hashing = True
        hash_name = 'queue_id'
        hashed_attrs = 'segment_id', 'import_method'
        hash_group = True
        
        definition = """
        -> Segment
        -> ImportMethod
        -> master
        """
        
        @classmethod
        def add_rows(cls, rows):
            cls.insert(rows, insert_to_master=True, ignore_extra_fields=True)

schema.spawn_missing_classes()
schema.connection.dependencies.load()
