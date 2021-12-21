"""
DataJoint tables for importing minnie65 from CAVE.
"""
import datajoint as dj
import datajoint.datajoint_plus as djp

from ..config import minnie65_materialization_config

minnie65_materialization_config.register_externals()
minnie65_materialization_config.register_adapters(context=locals())

schema = dj.schema(minnie65_materialization_config.schema_name, create_schema=True)

@schema
class Materialization(djp.Manual):
    definition = """
    # version and timestamp of Minnie65 materialization
    ver              : DECIMAL(6,2)    # materialization version
    ---
    valid=1          : tinyint         # marks whether the materialization is valid. Defaults to 1.
    timestamp        : timestamp       # marks the time at which the materialization service was started
    """
    
    class CurrentVersion(djp.Part):
        definition = """
        # version and timestamp of Minnie65 materialization
        kind       :  varchar(16)        # selection criteria for setting the current materialization
        ---
        -> master
        description=NULL                  : varchar(256)      # description of the materialization and source
        """    
    
    class Meta(djp.Part):
        definition = """
        -> master
        ---
        description=NULL                  : varchar(256)      # description of the materialization and source
        ts_inserted=CURRENT_TIMESTAMP     : timestamp         # timestamp that data was inserted into this DataJoint table.
        """


stable = Materialization.CurrentVersion & {'kind': 'stable'}
latest = Materialization.CurrentVersion & {'kind': 'latest'}
MICrONS_Mar_2021 = Materialization.CurrentVersion & {'kind': 'MICrONS_Mar_2021'}
MICrONS_Jul_2021 = Materialization.CurrentVersion & {'kind': 'MICrONS_Jul_2021'}
releaseJun2021 = Materialization.CurrentVersion & {'kind': 'release_Jun2021'}


@schema
class Nucleus(djp.Manual):
    definition = """
    # Nucleus detection from version 0 of the Allen Institute. Table name: 'nucleus_detection_v0'
    nucleus_id   : int unsigned   # id of nucleus from the flat segmentation  Equivalent to Allen: 'id'.
    ---
    """
    
    class Info(djp.Part):
        definition = """
        # Detailed information from each nucleus_id
        -> Materialization
        -> master     
        segment_id       : bigint unsigned    # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
        ---
        nucleus_x        : int unsigned       # x coordinate of nucleus centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
        nucleus_y        : int unsigned       # y coordinate of nucleus centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
        nucleus_z        : int unsigned       # z coordinate of nucleus centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
        supervoxel_id    : bigint unsigned    # id of the supervoxel under the nucleus centroid. Equivalent to Allen: 'pt_supervoxel_id'.
        volume=NULL      : float              # volume of the nucleus in um^3
        """
        
    class Meta(djp.Part):
        definition = """
        ->Materialization
        ---
        description=NULL                  : varchar(256)      # description of the table version
        ts_inserted=CURRENT_TIMESTAMP     : timestamp         # timestamp that data was inserted into this DataJoint table.
        """


@schema
class FunctionalCoreg(dj.Manual):
    definition = """
    # ID's of cells from the table 'functional_coreg'
    ->Materialization
    ->Nucleus
    segment_id       : bigint unsigned    # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
    scan_session     : smallint           # session index for the mouse 
    scan_idx         : smallint           # number of TIFF stack file
    unit_id          : int                # unit id from ScanSet.Unit
    ---
    centroid_x       : int unsigned       # x coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_y       : int unsigned       # y coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_z       : int unsigned       # z coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    supervoxel_id    : bigint unsigned    # id of the supervoxel under the nucleus centroid. Equivalent to Allen: 'pt_supervoxel_id'.
    """
        
    class Meta(dj.Part):
        definition = """
        ->Materialization
        ---
        description=NULL                  : varchar(256)      # description of the table version
        ts_inserted=CURRENT_TIMESTAMP     : timestamp         # timestamp that data was inserted into this DataJoint table.
        """ 


@schema
class ProofreadSegment(dj.Manual):
    definition = """
    # Segment ID's of manually proofread neurons from 'proofreading_functional_coreg_v2'
    ->Materialization
    segment_id       : bigint unsigned    # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
    """

    class Meta(dj.Part):
        definition = """
        ->Materialization
        ---
        description=NULL                  : varchar(256)      # description of the table version
        ts_inserted=CURRENT_TIMESTAMP     : timestamp         # timestamp that data was inserted into this DataJoint table.
        """ 


@schema
class ProofreadFunctionalCoregV2(dj.Manual):
    definition = """
    # ID's of cells from the table 'proofreading_functional_coreg_v2'
    ->Materialization
    ->Nucleus
    segment_id       : bigint unsigned    # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
    scan_session     : smallint           # session index for the mouse 
    scan_idx         : smallint           # number of TIFF stack file
    unit_id          : int                # unit id from ScanSet.Unit
    ---
    centroid_x       : int unsigned       # x coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_y       : int unsigned       # y coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_z       : int unsigned       # z coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    supervoxel_id    : bigint unsigned    # id of the supervoxel under the nucleus centroid. Equivalent to Allen: 'pt_supervoxel_id'.
    """
        
    class Meta(dj.Part):
        definition = """
        ->Materialization
        ---
        description=NULL                  : varchar(256)      # description of the table version
        ts_inserted=CURRENT_TIMESTAMP     : timestamp         # timestamp that data was inserted into this DataJoint table.
        """ 


@schema
class SynapseSegmentSource(dj.Manual):
    definition = """
    segment_id           : bigint unsigned              # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
    ---
    include=1            : tinyint                      # 1 if included in synapse source, 0 if excluded from synapse source
    """


@schema
class Synapse(dj.Computed):
    definition = """
    # Synapses from the table 'synapses_pni_2'
    ->SynapseSegmentSource.proj(primary_seg_id='segment_id')
    secondary_seg_id                              : bigint unsigned              # id of the segment that is synaptically paired to primary_segment_id.
    synapse_id                                    : bigint unsigned              # synapse index within the segmentation
    ---
    prepost                                       : varchar(16)                  # whether the primary_seg_id is "presyn" or "postsyn"
    synapse_x                                     : int unsigned                 # x coordinate of synapse centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm). From Allen 'ctr_pt_position'.
    synapse_y                                     : int unsigned                 # y coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm). From Allen 'ctr_pt_position'.
    synapse_z                                     : int unsigned                 # z coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm). From Allen 'ctr_pt_position'.
    synapse_size                                  : int unsigned                 # (EM voxels) scaled by (4x4x40)
    ts_inserted=CURRENT_TIMESTAMP                 : timestamp                    # timestamp that data was inserted into this DataJoint table.
    """


@schema
class AllenV1ColumnTypesSlanted(dj.Manual):
    definition = """
    # ID's of cells from the table 'allen_v1_column_types_slanted'
    ->Materialization
    ->Nucleus
    segment_id                : bigint unsigned    # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
    ---
    valid                     : tinyint            # 1 = valid entry, 0 = invalid entry
    classification_system     : varchar(128)       # method for classification
    cell_type                 : varchar(128)       # manually classified cell type
    n_nuc                     : smallint           # number of nuclei associated with segment_id (single somas n_nuc = 1)
    centroid_x                : int unsigned       # x coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_y                : int unsigned       # y coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_z                : int unsigned       # z coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    supervoxel_id             : bigint unsigned    # id of the supervoxel under the nucleus centroid. Equivalent to Allen: 'pt_supervoxel_id'.
    """
        
    class Meta(dj.Part):
        definition = """
        ->Materialization
        ---
        description=NULL                  : varchar(256)      # description of the table version
        ts_inserted=CURRENT_TIMESTAMP     : timestamp         # timestamp that data was inserted into this DataJoint table.
        """ 


@schema
class AllenSomaCourseCellClassModelV1(dj.Manual):
    definition = """
    # ID's of cells from the table 'allen_soma_coarse_cell_class_model_v1'
    ->Materialization
    ->Nucleus
    segment_id                : bigint unsigned    # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
    ---
    valid                     : tinyint            # 1 = valid entry, 0 = invalid entry
    classification_system     : varchar(128)       # method for classification
    cell_type                 : varchar(128)       # manually classified cell type
    n_nuc                     : smallint           # number of nuclei associated with segment_id (single somas n_nuc = 1)
    centroid_x                : int unsigned       # x coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_y                : int unsigned       # y coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_z                : int unsigned       # z coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    supervoxel_id             : bigint unsigned    # id of the supervoxel under the nucleus centroid. Equivalent to Allen: 'pt_supervoxel_id'.
    """
        
    class Meta(dj.Part):
        definition = """
        ->Materialization
        ---
        description=NULL                  : varchar(256)      # description of the table version
        ts_inserted=CURRENT_TIMESTAMP     : timestamp         # timestamp that data was inserted into this DataJoint table.
        """


@schema
class AllenSomaCourseCellClassModelV2(dj.Manual):
    definition = """
    # ID's of cells from the table 'allen_soma_coarse_cell_class_model_v2'
    ->Materialization
    ->Nucleus
    segment_id                : bigint unsigned    # id of the segment under the nucleus centroid. Equivalent to Allen 'pt_root_id'.
    ---
    valid                     : tinyint            # 1 = valid entry, 0 = invalid entry
    classification_system     : varchar(128)       # method for classification
    cell_type                 : varchar(128)       # manually classified cell type
    n_nuc                     : smallint           # number of nuclei associated with segment_id (single somas n_nuc = 1)
    centroid_x                : int unsigned       # x coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_y                : int unsigned       # y coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    centroid_z                : int unsigned       # z coordinate of centroid in EM voxels (x: 4nm, y: 4nm, z: 40nm)
    supervoxel_id             : bigint unsigned    # id of the supervoxel under the nucleus centroid. Equivalent to Allen: 'pt_supervoxel_id'.
    """
        
    class Meta(dj.Part):
        definition = """
        ->Materialization
        ---
        description=NULL                  : varchar(256)      # description of the table version
        ts_inserted=CURRENT_TIMESTAMP     : timestamp         # timestamp that data was inserted into this DataJoint table.
        """ 