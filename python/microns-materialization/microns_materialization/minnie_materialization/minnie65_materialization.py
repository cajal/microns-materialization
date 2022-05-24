import inspect
import json
import sys
from datetime import datetime
from pathlib import Path

import datajoint as dj
import datajoint_plus as djp
import numpy as np
import pandas as pd
import pcg_skel
from meshparty import trimesh_io

# Schema creation
from microns_materialization_api.schemas import \
    minnie65_materialization as m65mat

schema = m65mat.schema
config = m65mat.config

logger = djp.getLogger(__name__)

# Utils
from microns_utils.adapter_utils import adapt_mesh_hdf5
from microns_utils.ap_utils import set_CAVE_client
from microns_utils.filepath_utils import (append_timestamp_to_filepath,
                                          get_file_modification_time)
from microns_utils.misc_utils import wrap
from microns_utils.version_utils import \
    check_package_version_from_distributions as cpvfd

# TODO: Deal with filter out unrestricted

class ImportMethod(m65mat.ImportMethod):
    @classmethod
    def run(cls, key):
        return cls.r1p(key).run(**key)

    @classmethod
    def validate_method(cls, names, method_values, current_values):
        results = []
        for name, mv, cv in zip(wrap(names), wrap(method_values), wrap(current_values)):
            if mv != cv:
                cls.Log('error', f"This method requires {name} to be {mv}, but currently is {cv}. Create a new method.")
                results.append(0)
            else:
                results.append(1)
        assert np.all(results), 'Method compatibility validation failed. Check logs.'

    class MaterializationVer(m65mat.ImportMethod.MaterializationVer):
        @classmethod
        def update_method(cls, ver=None, **kwargs):
            cls.Log('info', f'Updating method for {cls.class_name}.')
            
            # DEFAULTS
            datastack = 'minnie65_phase3_v1'
            
            # INSERT
            client = set_CAVE_client(datastack, ver)
            cls.insert1({
                'caveclient_version': cpvfd('caveclient'),
                'datastack': datastack,
                'ver': ver if ver is not None else client.materialize.version,
            }, ignore_extra_fields=True, insert_to_master=True, skip_duplicates=True)
        
        def run(self, **kwargs):
            params = (self & kwargs).fetch1()
            self.Log('info', f'Running {self.class_name} with params {params}.')

            # INITIALIZE & VALIDATE
            client = set_CAVE_client(params['datastack'], ver=params['ver'])
            self.master.validate_method(
                names=('caveclient version', 'datastack', 'materialization_version'),
                method_values=(params['caveclient_version'], params['datastack'], params['ver']),
                current_values=(cpvfd('caveclient'), client.materialize.datastack_name, client.materialize.version)
            )

            # IMPORT DATA
            data = client.materialize.get_version_metadata()
            data.update({'ver': data['version']})
            return data
    
    class NucleusSegment(m65mat.ImportMethod.NucleusSegment):
        @classmethod
        def update_method(cls, ver=None, **kwargs):
            cls.Log('info', f'Updating method for {cls.class_name}.')

            # DEFAULTS
            datastack = 'minnie65_phase3_v1'

            # INSERT
            client = set_CAVE_client(datastack, ver)
            cls.insert1({
                'caveclient_version': cpvfd('caveclient'),
                'datastack': datastack,
                'ver': ver if ver is not None else client.materialize.version,
            }, ignore_extra_fields=True, skip_duplicates=True, insert_to_master=True)

        def run(self, **kwargs):
            params = (self & kwargs).fetch1()
            self.Log('info', f'Running {self.class_name} with params {params}.')
            
            # INITIALIZE & VALIDATE
            client = set_CAVE_client(params['datastack'], ver=params['ver'])
            self.master.validate_method(
                names=('caveclient version', 'datastack', 'materialization_version'),
                method_values=(params['caveclient_version'], params['datastack'], params['ver']),
                current_values=(cpvfd('caveclient'), client.materialize.datastack_name, client.materialize.version)
            )

            # IMPORT DATA
            df = client.materialize.query_table('nucleus_detection_v0')
            rename_dict = {
                'id': 'nucleus_id',
                'pt_root_id': 'segment_id',
                'pt_supervoxel_id': 'supervoxel_id'
            }
            df = df.rename(columns=rename_dict)
            df['ver'] = params['ver']
            df['nucleus_x'], df['nucleus_y'], df['nucleus_z'] = np.stack(df.pt_position.values).T
            df['import_method'] = params['import_method']
            return {'df': df}
    
    class MeshPartyMesh(m65mat.ImportMethod.MeshPartyMesh):
        @classmethod
        def update_method(cls, *args, **kwargs):
            msg = f'{cls.class_name} has been deprecated. Use {cls.master.class_name}.MeshPartyMesh2.'
            cls.Log('error', msg)
            raise Exception(msg)

        def run(self, *args, **kwargs):
            msg = f'{self.class_name} has been deprecated. Use {self.master.class_name}.MeshPartyMesh2.'
            self.Log('error', msg)
            raise Exception(msg)

    class MeshPartyMesh2(m65mat.ImportMethod.MeshPartyMesh2):
        @classmethod
        def update_method(cls, ver=None, download_meshes_kwargs={}, **kwargs):
            cls.Log('info', f'Updating method for {cls.class_name}.')
            
            # DEFAULTS
            datastack = 'minnie65_phase3_v1'           
            download_meshes_kwargs.setdefault('overwrite', False)
            download_meshes_kwargs.setdefault('n_threads', 10)
            download_meshes_kwargs.setdefault('verbose', False)
            download_meshes_kwargs.setdefault('stitch_mesh_chunks', True)
            download_meshes_kwargs.setdefault('merge_large_components', False)
            download_meshes_kwargs.setdefault('remove_duplicate_vertices', True)
            download_meshes_kwargs.setdefault('map_gs_to_https', True)
            download_meshes_kwargs.setdefault('fmt', "hdf5")
            download_meshes_kwargs.setdefault('save_draco', False)
            download_meshes_kwargs.setdefault('chunk_size', None)
            download_meshes_kwargs.setdefault('progress', False)

            # INSERT
            client = set_CAVE_client(datastack, ver)
            cls.insert1(
                {
                    'description' : '',
                    'meshparty_version': cpvfd('meshparty'),
                    'cloudvolume_version': cpvfd('cloud-volume'),
                    'caveclient_version': cpvfd('caveclient'),
                    'datastack': datastack,
                    'ver': ver if ver is not None else client.materialize.version,
                    'cloudvolume_path': client.info.segmentation_source(),
                    'download_meshes_kwargs': json.dumps(download_meshes_kwargs),
                    'target_dir': config.externals['minnie65_meshes']['location'],
                    
                },
                insert_to_master=True, 
                skip_duplicates=True, 
            )

        def run(self, **kwargs):
            params = (self & kwargs).fetch1()
            self.Log('info', f'Running {self.class_name} with params {params}.')

            # INITIALIZE & VALIDATE
            client = set_CAVE_client(params['datastack'], params['ver'])
            packages = {
                'meshparty_version': 'meshparty',
                'caveclient_version': 'caveclient',
                'cloudvolume_version': 'cloud-volume'
            }
            self.master.validate_method(
                names=list(packages.keys()) + ['cloudvolume_path', 'datastack', 'materialization_version'],
                method_values=[params[k] for k in packages.keys()] + [params['cloudvolume_path'], params['datastack'], params['ver']],
                current_values=[cpvfd(v) for v in packages.values()] + [client.info.segmentation_source(), client.materialize.datastack_name, client.materialize.version]
            )

            # IMPORT DATA
            segment_id = kwargs['segment_id']
            target_dir = params['target_dir']

            trimesh_io.download_meshes(seg_ids=wrap(segment_id), target_dir=target_dir, cv_path=params['cloudvolume_path'], **json.loads(params['download_meshes_kwargs']))

            # make file path
            filepath = Path(target_dir).joinpath(str(segment_id)).with_suffix('.h5')

            # append timestamp to filepath 
            ts_computed = get_file_modification_time(filepath, timezone='US/Central', fmt="%Y-%m-%d_%H:%M:%S")
            filepath = append_timestamp_to_filepath(filepath, ts_computed, return_filepath=True)
            
            # get mesh data
            n_vertices, n_faces, info_dict = adapt_mesh_hdf5(filepath=filepath, parse_filepath_stem=True, filepath_has_timestamp=True, separator='__', as_lengths=True)
            assert kwargs['segment_id'] == info_dict['segment_id'], 'segment_id in filepath does not match provided segment_id.' # sanity check
            
            info_dict['ts_computed'] = str(info_dict.pop('timestamp'))
            info_dict['mesh'] = info_dict.pop('filepath')

            return {'n_vertices': n_vertices, 'n_faces': n_faces, **info_dict}
    
    class Synapse(m65mat.ImportMethod.Synapse):
        @classmethod
        def update_method(cls, *args, **kwargs):
            msg = f'{cls.class_name} has been deprecated. Use {cls.master.class_name}.Synapse2.'
            cls.Log('error', msg)
            raise Exception(msg)

        def run(self, *args, **kwargs):
            msg = f'{self.class_name} has been deprecated. Use {self.master.class_name}.Synapse2.'
            self.Log('error', msg)
            raise Exception(msg)
    
    class Synapse2(m65mat.ImportMethod.Synapse2):
        @classmethod
        def update_method(cls, ver=None, **kwargs):
            cls.Log('info', f'Updating method for {cls.class_name}.')

            # DEFAULTS            
            datastack = 'minnie65_phase3_v1'

            # INSERT
            client = set_CAVE_client(datastack, ver)
            cls.insert1({
                'caveclient_version': cpvfd('caveclient'),
                'datastack': datastack,
                'ver': ver if ver is not None else client.materialize.version,
            }, ignore_extra_fields=True, skip_duplicates=True, insert_to_master=True)

        def run(self, **kwargs):
            params = (self & kwargs).fetch1()
            self.Log('info', f'Running {self.class_name} with params {params}.')
            
            # INITIALIZE & VALIDATE
            client = set_CAVE_client(params['datastack'], ver=params['ver'])
            self.master.validate_method(
                names=('caveclient version', 'datastack', 'materialization_version'),
                method_values=(params['caveclient_version'], params['datastack'], params['ver']),
                current_values=(cpvfd('caveclient'), client.materialize.datastack_name, client.materialize.version)
            )

            # IMPORT DATA
            primary_seg_id = int(kwargs['primary_seg_id'])

            # get synapses where primary segment is presynaptic
            df_pre = client.materialize.query_table('synapses_pni_2', filter_equal_dict={'pre_pt_root_id': primary_seg_id})
            df_pre = df_pre.rename(columns={'pre_pt_root_id':'primary_seg_id', 'post_pt_root_id': 'secondary_seg_id'})
            df_pre['prepost'] = 'presyn'
            
            # get synapses where primary segment is postsynaptic
            df_post = client.materialize.query_table('synapses_pni_2', filter_equal_dict={'post_pt_root_id': primary_seg_id})
            df_post = df_post.rename(columns={'post_pt_root_id':'primary_seg_id', 'pre_pt_root_id': 'secondary_seg_id'})
            df_post['prepost'] = 'postsyn'
            
            # combine dataframes
            df = pd.concat([df_pre, df_post], axis=0)
            
            # remove autapses (these are mostly errors)
            df = df[df['primary_seg_id']!=df['secondary_seg_id']]
            
            if len(df)>0:
                # add synapse_xyz
                df['synapse_x'], df['synapse_y'], df['synapse_z'] = np.stack(df['ctr_pt_position'].T, -1)
                rename_dict = {
                    'id': 'synapse_id', 
                    'size':'synapse_size',
                }
                df = df.rename(columns=rename_dict)[['primary_seg_id', 'secondary_seg_id', 'synapse_id', \
                                                        'prepost', 'synapse_x', 'synapse_y', 'synapse_z', 'synapse_size']]
                
                df['import_method'] = params['import_method']
                
                return {'df': df}
            else:
                return {'df': []}

    class PCGMeshwork(m65mat.ImportMethod.PCGMeshwork):
        @classmethod
        def update_method(cls, ver=None, pcg_meshwork_params={}, **kwargs):
            cls.Log('info', f'Updating method for {cls.class_name}.')
            
            # DEFAULTS
            datastack = 'minnie65_phase3_v1'
            pcg_meshwork_params.setdefault('n_parallel', 10)
            pcg_meshwork_params.setdefault('refine', 'all')
            pcg_meshwork_params.setdefault('collapse_soma', True)
            pcg_meshwork_params.setdefault('synapses', 'all')
            pcg_meshwork_params.setdefault('root_point_resolution', [4,4,40])

            # INSERT
            client = set_CAVE_client(datastack, ver)
            cls.insert1(
                {
                    'meshparty_version': cpvfd('meshparty'),
                    'caveclient_version': cpvfd('caveclient'),
                    'pcg_skel_version': cpvfd('pcg-skel'),
                    'datastack': datastack,
                    'ver': ver if ver is not None else client.materialize.version,
                    'synapse_table': 'synapses_pni_2',
                    'nucleus_table': 'nucleus_detection_v0',
                    'cloudvolume_version': cpvfd('cloud-volume'),
                    'cloudvolume_path': client.info.segmentation_source(),
                    'pcg_meshwork_params': json.dumps(pcg_meshwork_params),
                    'target_dir': config.externals['minnie65_meshwork']['location'],
                    
                },
                insert_to_master=True, 
                skip_duplicates=True, 
            )

        def run(self, **kwargs):
            params = (self & kwargs).fetch1()
            self.Log('info', f'Running {self.class_name} with params {params}.')

            # INITIALIZE & VALIDATE
            client = set_CAVE_client(params['datastack'], ver=params['ver'])

            # validate package dependencies
            packages = {
                'meshparty_version': 'meshparty',
                'pcg_skel_version': 'pcg-skel',
                'caveclient_version': 'caveclient',
                'cloudvolume_version': 'cloud-volume'
            }

            self.master.validate_method(
                names=list(packages.keys()) + ['cloudvolume_path', 'datastack', 'materialization_version'],
                method_values=[params[k] for k in packages.keys()] + [params['cloudvolume_path'], params['datastack'], params['ver']],
                current_values=[cpvfd(v) for v in packages.values()] + [client.info.segmentation_source(), client.materialize.datastack_name, client.materialize.version]
            )

            # IMPORT DATA
            segment_id = int(kwargs['segment_id'])
            synapse_table = params['synapse_table']
            nucleus_table = params['nucleus_table']
            target_dir = params['target_dir']
            pcg_meshwork_params = json.loads(params['pcg_meshwork_params'])

            # check that segment has synapses
            n_presyn = len(client.materialize.query_table(synapse_table, filter_equal_dict={'pre_pt_root_id': segment_id}))
            n_postsyn = len(client.materialize.query_table(synapse_table, filter_equal_dict={'pre_pt_root_id': segment_id}))
            if not (n_presyn > 0 or n_postsyn > 0):
                self.Log('info', f'No synapses found for segment_id {segment_id} in {synapse_table}.')
                return {'meshwork_obj': []}

            # check if segment has nucleus
            nuc_df = client.materialize.query_table(nucleus_table, filter_equal_dict={'pt_root_id': segment_id})
            if len(nuc_df) > 0: 
                soma_centroid = nuc_df.pt_position.values[0]
            else: 
                self.Log('info', f'No nucleus found for segment_id {segment_id} in {nucleus_table}.')
                soma_centroid = None

            # download meshwork obj
            meshwork_obj = pcg_skel.pcg_meshwork(
                root_id=segment_id,
                client=client,
                root_point=soma_centroid,
                synapse_table=synapse_table,
                **pcg_meshwork_params
            )

            # make file path
            filepath = Path(target_dir).joinpath(str(segment_id)).with_suffix('.h5')

            # save meshwork file
            meshwork_obj.save_meshwork(filepath)

            # append timestamp to filepath 
            ts_computed = get_file_modification_time(filepath, timezone='US/Central', fmt="%Y-%m-%d_%H:%M:%S")
            filepath = append_timestamp_to_filepath(filepath, ts_computed, return_filepath=True)
            
            return {
                'segment_id': segment_id, 
                'import_method': params['import_method'], 
                'meshwork_obj': filepath, 
                'ts_computed': ts_computed
            }

    class PCGSkeleton(m65mat.ImportMethod.PCGSkeleton):
        @classmethod
        def update_method(cls, ver=None, pcg_skel_params={}, **kwargs):
            cls.Log('info', f'Updating method for {cls.class_name}.')

            # DEFAULTS
            datastack = 'minnie65_phase3_v1'
            pcg_skel_params.setdefault('n_parallel', 10)
            pcg_skel_params.setdefault('refine', 'all')
            pcg_skel_params.setdefault('collapse_soma', True)
            pcg_skel_params.setdefault('root_point_resolution', [4,4,40])

            # INSERT
            client = set_CAVE_client(datastack, ver)
            cls.insert1(
                {
                    'meshparty_version': cpvfd('meshparty'),
                    'caveclient_version': cpvfd('caveclient'),
                    'pcg_skel_version': cpvfd('pcg-skel'),
                    'datastack': datastack,
                    'ver': ver if ver is not None else client.materialize.version,
                    'synapse_table': 'synapses_pni_2',
                    'nucleus_table': 'nucleus_detection_v0',
                    'cloudvolume_version': cpvfd('cloud-volume'),
                    'cloudvolume_path': client.info.segmentation_source(),
                    'pcg_skel_params': json.dumps(pcg_skel_params),
                    'target_dir': config.externals['minnie65_pcg_skeletons']['location'],
                    
                },
                insert_to_master=True, 
                ignore_extra_fields=True,
                skip_duplicates=True, 
            )
            
        def run(self, **kwargs):
            params = (self & kwargs).fetch1()
            self.Log('info', f'Running {self.class_name} with params {params}.')

            # INITIALIZE & VALIDATE
            client = set_CAVE_client(params['datastack'], ver=params['ver'])

            # validate package dependencies
            packages = {
                'meshparty_version': 'meshparty',
                'pcg_skel_version': 'pcg-skel',
                'caveclient_version': 'caveclient',
                'cloudvolume_version': 'cloud-volume'
            }

            self.master.validate_method(
                names=list(packages.keys()) + ['cloudvolume_path', 'datastack', 'materialization_version'],
                method_values=[params[k] for k in packages.keys()] + [params['cloudvolume_path'], params['datastack'], params['ver']],
                current_values=[cpvfd(v) for v in packages.values()] + [client.info.segmentation_source(), client.materialize.datastack_name, client.materialize.version]
            )
            
            # IMPORT DATA
            segment_id = int(kwargs['segment_id'])
            nucleus_table = params['nucleus_table']
            target_dir = params['target_dir']
            pcg_skel_params = json.loads(params['pcg_skel_params'])

            # check if segment has nucleus
            nuc_df = client.materialize.query_table(nucleus_table, filter_equal_dict={'pt_root_id': segment_id})
            if len(nuc_df) > 0: 
                soma_centroid = nuc_df.pt_position.values[0]
            else:
                self.Log('info', f'No nucleus found for segment_id {segment_id} in {nucleus_table}.')
                soma_centroid = None

            # download skeleton obj
            skeleton_obj = pcg_skel.pcg_skeleton(
                root_id=segment_id,
                client=client,
                root_point=soma_centroid,
                **pcg_skel_params
            )

            # make file path
            filepath = Path(target_dir).joinpath(str(segment_id)).with_suffix('.h5')

            # save skeleton file
            skeleton_obj.write_to_h5(filepath)

            # append timestamp to filepath 
            ts_computed = get_file_modification_time(filepath, timezone='US/Central', fmt="%Y-%m-%d_%H:%M:%S")
            filepath = append_timestamp_to_filepath(filepath, ts_computed, return_filepath=True)
            
            return {
                'segment_id': segment_id, 
                'import_method': params['import_method'], 
                'skeleton_obj': filepath, 
                'ts_computed': ts_computed
            }

class Materialization(m65mat.Materialization):
    
    class Info(m65mat.Materialization.Info): pass
    
    class Checkpoint(m65mat.Materialization.Checkpoint):
        @classmethod
        def add_checkpoint(cls, name, ver, description):
            cls.insert1({
                'name': name,
                'ver': ver,
                'description': description
                }
            )

        @classmethod
        def fill_mat_v1(cls):
            cls.configure_logger()
            mat_v1 = dj.create_virtual_module('mat_v1', 'microns_minnie65_materialization')
            rel = mat_v1.Materialization.CurrentVersion & [{'kind': 'MICrONS_Jul_2021'}, {'kind': 'MICrONS_Mar_2021'}, {'kind': 'release_Jun2021'}]
            cls.insert(rel.proj(..., name='kind'))
    
    class MatV1(m65mat.Materialization.MatV1):
        @property
        def key_source(self):
            return dj.create_virtual_module('mat_v1', 'microns_minnie65_materialization').Materialization
        
        def get(self, key):
            return (self.key_source & key).fetch1()
        
        def make(self, key):
            row = self.get(key)
            row.update({'time_stamp': row['timestamp'], 'datastack': 'minnie65_phase3_v1'})
            self.master.insert1(row, ignore_extra_fields=True, skip_duplicates=True)
            self.master.Info.insert1(row, ignore_extra_fields=True, skip_duplicates=True)
            self.insert1(row, ignore_extra_fields=True, skip_duplicates=True)
    
    class CAVE(m65mat.Materialization.CAVE):
        @property
        def key_source(self):
            return ImportMethod.MaterializationVer - Materialization
        
        def make(self, key):
            result = {**key, **ImportMethod.run(key)}
            Materialization.insert1(result, ignore_extra_fields=True, skip_duplicates=True)
            Materialization.Info.insert1(result, ignore_extra_fields=True, skip_duplicates=True)
            self.insert1(result, insert_to_master=True, ignore_extra_fields=True, skip_duplicates=True)


class Nucleus(m65mat.Nucleus):
    
    class Info(m65mat.Nucleus.Info): pass
    
    class MatV1(m65mat.Nucleus.MatV1):
        @classmethod
        def fill(cls):
            cls.configure_logger()
            m65mat_v1 = dj.create_virtual_module('mat_v1', 'microns_minnie65_materialization')
            with dj.conn().transaction:
                cls.master.insert(m65mat_v1.Nucleus.Info, ignore_extra_fields=True, skip_duplicates=True)
                cls.master.Info.insert(m65mat_v1.Nucleus.Info, ignore_extra_fields=True, skip_duplicates=True)
                cls.insert(m65mat_v1.Nucleus.Info, ignore_extra_fields=True, skip_duplicates=True)
    
    class CAVE(m65mat.Nucleus.CAVE):
        @property
        def key_source(self):
            return ImportMethod.NucleusSegment & (Materialization & (Materialization.CAVE - Nucleus.Info.proj()))
        
        def make(self, key):
            df = ImportMethod.run(key)['df']
            self.master.insert(df, ignore_extra_fields=True, skip_duplicates=True)
            self.master.Info.insert(df, ignore_extra_fields=True, skip_duplicates=True)
            self.insert(df, insert_to_master=True, ignore_extra_fields=True, skip_duplicates=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})


class Segment(m65mat.Segment):

    class MatV1(m65mat.Segment.MatV1):       
        @classmethod
        def fill(cls):
            cls.master.insert(Nucleus.MatV1, ignore_extra_fields=True, skip_duplicates=True)
            cls.insert(Nucleus.MatV1, ignore_extra_fields=True, skip_duplicates=True)
    
    class Nucleus(m65mat.Segment.Nucleus):
        @property
        def key_source(self):
            return Materialization & Nucleus.Info()
        
        def make(self, key):
            self.master.insert(Nucleus.Info & key, ignore_extra_fields=True, skip_duplicates=True)
            self.insert(Nucleus.Info & key, ignore_extra_fields=True, skip_duplicates=True)


class Exclusion(m65mat.Exclusion): pass


class Synapse(m65mat.Synapse):

    class Info(m65mat.Synapse.Info): pass
    
    class MatV1(m65mat.Synapse.MatV1):
        @classmethod
        def fill(cls):
            m65mat_v1 = dj.create_virtual_module('mat_v1', 'microns_minnie65_materialization')
            with dj.conn().transaction:
                cls.master.insert(m65mat_v1.Synapse, ignore_extra_fields=True, skip_duplicates=True)
                cls.master.Info.insert(m65mat_v1.Synapse, ignore_extra_fields=True, skip_duplicates=True)
                cls.insert(m65mat_v1.Synapse, ignore_extra_fields=True, skip_duplicates=True)

    class SegmentExclude(m65mat.Synapse.SegmentExclude): pass

    class CAVE(m65mat.Synapse.CAVE):
        @property
        def key_source(self):
            return (Segment.proj(primary_seg_id='segment_id') - Synapse.Info - Synapse.SegmentExclude) * ImportMethod.Synapse2

        def make(self, key):
            df = ImportMethod.run(key)['df']
            if len(df) > 0:
                self.master.insert(df, ignore_extra_fields=True, skip_duplicates=True)
                self.master.Info.insert(df, ignore_extra_fields=True, skip_duplicates=True)
                self.insert(df, ignore_extra_fields=True)
            else:
                self.master.SegmentExclude.insert1({'primary_seg_id': key['primary_seg_id'], 'synapse_id': 0, Exclusion.hash_name: Exclusion.hash1({'reason': 'no synapse data'})}, skip_duplicates=True)


class Mesh(m65mat.Mesh):
    
    class Object(m65mat.Mesh.Object): pass
    
    class MeshParty(m65mat.Mesh.MeshParty):
        @property
        def key_source(self):
            return ((Segment & Segment.Nucleus & 'segment_id!= 0')  - Mesh.MeshParty.proj()) * ImportMethod.MeshPartyMesh2
        
        def make(self, key):
            result = {**key, **ImportMethod.run(key)}
            result = {**{self.hash_name: self.hash1(result)}, **result}
            self.master.insert1(result, ignore_extra_fields=True, skip_duplicates=True)
            self.master.Object.insert1(result, ignore_extra_fields=True, skip_duplicates=True)
            self.insert1(result, insert_to_master=True, skip_hashing=True, ignore_extra_fields=True, skip_duplicates=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})


class Meshwork(m65mat.Meshwork):

    class PCGMeshwork(m65mat.Meshwork.PCGMeshwork): pass

    class PCGMeshworkExclude(m65mat.Meshwork.PCGMeshworkExclude): pass
        
    class PCGMeshworkMaker(m65mat.Meshwork.PCGMeshworkMaker):
        @property
        def key_source(self):
            return ((Segment & Segment.Nucleus & 'segment_id!= 0')  - Meshwork.PCGMeshworkMaker.proj() - Meshwork.PCGMeshworkExclude.proj()) * ImportMethod.PCGMeshwork.get_latest_entries()
        
        def make(self, key):
            result = {**key, **ImportMethod.run(key)}
            if result['meshwork_obj']:
                result = {**{self.hash_name: self.hash1(result)}, **result}
                self.master.insert1(result, ignore_extra_fields=True, skip_duplicates=True)
                self.master.PCGMeshwork.insert1(result, ignore_extra_fields=True, skip_duplicates=True)
                self.insert1(result, insert_to_master=True, skip_hashing=True, ignore_extra_fields=True, skip_duplicates=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})
            else:
                self.master.PCGMeshworkExclude.insert1({'segment_id': key['segment_id'], 'meshwork_id': 0, Exclusion.hash_name: Exclusion.hash1({'reason': 'no synapse data'}), 'ts_computed': str(datetime.now())}, ignore_extra_fields=True, skip_duplicates=True)


class Skeleton(m65mat.Skeleton):

    class PCGSkeleton(m65mat.Skeleton.PCGSkeleton): pass
        
    class PCGSkeletonMaker(m65mat.Skeleton.PCGSkeletonMaker):
        @property
        def key_source(self):
            return ((Segment & Segment.Nucleus & 'segment_id!= 0')  - Skeleton.PCGSkeletonMaker.proj()) * ImportMethod.PCGSkeleton.get_latest_entries()

        def make(self, key):
            result = {**key, **ImportMethod.run(key)}
            result = {**{self.hash_name: self.hash1(result)}, **result}
            self.master.insert1(result, ignore_extra_fields=True, skip_duplicates=True)
            self.master.PCGSkeleton.insert1(result, ignore_extra_fields=True, skip_duplicates=True)
            self.insert1(result, insert_to_master=True, skip_hashing=True, ignore_extra_fields=True, skip_duplicates=True, insert_to_master_kws={'ignore_extra_fields': True, 'skip_duplicates': True})


class Queue(m65mat.Queue):

    class PCGMeshwork(m65mat.Queue.PCGMeshwork): pass
    
    class PCGSkeleton(m65mat.Queue.PCGSkeleton): pass


def update_log_level(loglevel, update_root_level=True):
    """
    Updates module and root logger.

    :param loglevel: (str) desired log level to overwrite default
    :param update_root_level: (bool) updates root level with provided loglevel
    """
    logger = djp.getLogger(__name__, level=loglevel, update_root_level=update_root_level)
    logger.info(f'Logging level set to {loglevel}.')
    
    # update tables in module
    # TODO - write this recursively
    logger.info('Updating loglevel for all tables.')
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if name in ['key_source', '_master', 'master', 'UserTable']:
            continue
        if hasattr(obj, 'loglevel'):
            obj.loglevel = loglevel
            for partname, subobj in inspect.getmembers(obj):
                if partname in ['key_source', '_master', 'master', 'UserTable']:
                    continue
                if hasattr(subobj, 'loglevel'):
                    subobj.loglevel = loglevel


def download_materialization(ver=None, download_synapses=False, download_meshes=False, loglevel=None, update_root_level=True):
    """
    Downloads materialization from CAVE.

    :param ver: (int) materialization version to download
        If None, latest materialization is downloaded.
    :param loglevel: (str) Optional, desired log level to overwrite default
    :param update_root_level: (bool) updates root level with provided loglevel
    """
    logger.info(f'Materialization download initialized.')
    
    if loglevel is not None:
        update_log_level(loglevel=loglevel, update_root_level=update_root_level)

    methods = [
        ImportMethod.MaterializationVer,
        ImportMethod.NucleusSegment,
    ]

    makers = [
        Materialization.CAVE,
        Nucleus.CAVE,
        Segment.Nucleus,
    ]

    if download_synapses:
        methods += [ImportMethod.Synapse2]
        makers += [Synapse.CAVE]

    if download_meshes:
        methods += [ImportMethod.MeshPartyMesh2]
        makers += [Mesh.MeshParty]

    for m in methods:
        logger.info(f'Updating methods for {m.class_name}.')
        m.update_method(ver=ver)

    for mk in makers:
        logger.info(f'Populating {mk.class_name}.')
        mk.populate(m.master & m.get_latest_entries(), reserve_jobs=True, order='random', suppress_errors=True)


def download_meshwork_objects(restriction={}, loglevel=None, update_root_level=True):
    """
    Downloads meshwork objects from cloud-volume.

    :param restriction: restriction to pass to populate
    :param loglevel: (str) Optional, desired log level to overwrite default
    :param update_root_level: (bool) updates root level with provided loglevel
    """        
    logger.info(f'Meshwork download initialized.')

    if loglevel is not None:
        update_log_level(loglevel=loglevel, update_root_level=update_root_level)

    Meshwork.PCGMeshworkMaker.populate(restriction, reserve_jobs=True, order='random', suppress_errors=True)


def download_pcg_skeletons(restriction={}, loglevel=None, update_root_level=True):
    """
    Downloads meshwork objects from cloud-volume.

    :param restriction: restriction to pass to populate
    :param loglevel: (str) Optional, desired log level to overwrite default
    :param update_root_level: (bool) updates root level with provided loglevel
    """        
    logger.info(f'Meshwork download initialized.')

    if loglevel is not None:
        update_log_level(loglevel=loglevel, update_root_level=update_root_level)

    Skeleton.PCGSkeletonMaker.populate(restriction, reserve_jobs=True, order='random', suppress_errors=True)
