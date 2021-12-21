import datajoint as dj
from datajoint import datajoint_plus as djp
from caveclient import CAVEclient


import numpy as np
import datetime
import pandas as pd
import traceback
import sys
from pathlib import Path
import time

if 'ipykernel' in sys.modules:
    from tqdm import tqdm_notebook as tqdm
else:
    from tqdm import tqdm

# Schema creation
from microns_materialization_api.schemas import minnie65_materialization as m65mat

class Materialization(m65mat.Materialization):
    
    class CurrentVersion(m65mat.Materialization.CurrentVersion): pass   
    
    class Meta(m65mat.Materialization.Meta): pass

    @classmethod
    def fill(cls, client, ver, description=None, update_latest_tag=True):
        print('--> Fetching materialization timestamp...')
        
        # get materialization timestamp
        ts = client.materialize.get_version_metadata()['time_stamp'] 
        
        print('Inserting data...')
        # insert materialization version and timestamp
        cls.insert1({'ver': client.materialize.version, 'timestamp': ts})
        
        # insert metadata
        Materialization.Meta.insert1({'ver': client.materialize.version, 'description': description})
        print(f'Successfully inserted timestamp: {ts} for materialization version: {client.materialize.version:.2f} with description: "{description}"')        
        
        # Update latest tag
        if update_latest_tag:
            if ver == -1:
                try: 
                    Materialization.CurrentVersion.insert1({'kind': 'latest', 'ver': client.materialize.version})
                    print(f'Set Materialization.CurrentVersion "latest" to materialization version {client.materialize.version:.2f}')

                except:
                    (Materialization.CurrentVersion & 'kind="latest"')._update('ver', client.materialize.version)
                    print(f'Updated Materialization.CurrentVersion "latest" to materialization version {client.materialize.version:.2f}')


stable = m65mat.stable
latest = m65mat.latest
MICrONS_Mar_2021 = m65mat.MICrONS_Mar_2021
MICrONS_Jul_2021 = m65mat.MICrONS_Jul_2021
releaseJun2021 = m65mat.releaseJun2021

def _version_mgr(client, ver, table=Materialization):
    """ Modify the client to the user specified materialization version and check if the version is already in the DataJoint table. 
       
    :param client: CAVEclient object that will be modified by _version_mgr.
    
    :param ver: User specified materialization version.
    
    returns: 
        code : 0 if the version already exists in the table
             : 1 if the version does not exist in the table
        
        client :  the client set to the user specified materialization version
    """
    code=None
    
    # set version if desired version is not latest 
    if ver != -1:
        if ver == client.materialize.most_recent_version():
            print(f'Materialization version {ver:.2f} is currently the latest version.')
        
        else:
            client.materialize._version = ver
            print(f'Materialization version set to {client.materialize.version:.2f}. This is not the latest version.') 
    
    else:
        print(f'Checking for new materialization...') 
    
    # check if version exists in table
    if len(table.Meta & f'ver={client.materialize.version}') > 0:
        code = 0
        print(f'Materialization version {client.materialize.version:.2f} already in schema.')
        return code, client

    else:
        code = 1
        print(f'Found new materialization version: {client.materialize.version:.2f}')
        return code, client



class Nucleus(m65mat.Nucleus):
    
    class Info(m65mat.Nucleus.Info): pass
        
    class Meta(m65mat.Nucleus.Meta): pass
    
    @classmethod
    def fetch_df(cls, client):
        print(f'Fetching data for materialization version {client.materialize.version:.2f}... ')
        # fetch nucleus dataframe
        nuc_df = client.materialize.query_table('nucleus_detection_v0')
        
        # reformat nucleus dataframe
        rename_dict = {
            'id': 'nucleus_id',
            'pt_root_id': 'segment_id',
            'pt_supervoxel_id': 'supervoxel_id'
        }
        
        nuc_copy = nuc_df[['id', 'pt_supervoxel_id', 'pt_root_id', 'volume']].copy().rename(columns=rename_dict)
        nuc_copy['ver'] = client.materialize.version
        nuc_copy['nucleus_x'], nuc_copy['nucleus_y'], nuc_copy['nucleus_z'] = np.stack(nuc_df.pt_position.values).T
        return nuc_copy
    
    @classmethod
    def fill_synapse_segment_source(cls):
        print('--> Filling SynapseSegmentSource table...')
        to_insert = (dj.U('segment_id') & (cls.Info & 'segment_id>0')) - SynapseSegmentSource.proj()
        SynapseSegmentSource.insert(to_insert)
        print(f'Inserted {len(to_insert)} new segments.')

    @classmethod
    def fill(cls, client, description=None):
        print('--> Filling Nucleus table...')
        
        # fetch data from materialization service
        df = cls.fetch_df(client)
        
        # insert nucleus id to master table
        cls.insert(df[['nucleus_id']].to_records(index=False), skip_duplicates=True)
            
        print('Inserting data...')
        # insert nucleus dataframe to Info
        cls.Info.insert(df.to_records(index=False))
        
        # insert metadata
        cls.Meta.insert1({'ver': client.materialize.version, 'description': description})
        print(f'Successfully inserted nucleus information for materialization version: {client.materialize.version:.2f}')



class FunctionalCoreg(m65mat.FunctionalCoreg):
        
    class Meta(m65mat.FunctionalCoreg.Meta): pass
    
    @classmethod
    def fetch_df(cls, client):
        print(f'Fetching data for materialization version {client.materialize.version:.2f}... ')
        
        # fetch dataframes
        fc_df = client.materialize.query_table('functional_coreg', filter_out_dict={'pt_root_id': [0]})
        nuc_df = client.materialize.query_table('nucleus_detection_v0', filter_out_dict={'pt_root_id': [0]})
        
        # merge proofread and nucleus dataframes
        rename_dict = {
            'id_x': 'nucleus_id', 
            'pt_root_id': 'segment_id', 
            'pt_position_y':'centroid', 
            'pt_supervoxel_id_y': 'supervoxel_id',
            'session': 'scan_session'
        }
        fc_nuc_df_orig = pd.merge(nuc_df, fc_df, on='pt_root_id').rename(columns=rename_dict)
        fc_nuc_df = fc_nuc_df_orig[['nucleus_id', 'segment_id', 'supervoxel_id', 'scan_session', 'scan_idx', 'unit_id']].copy()
        fc_nuc_df['ver'] = client.materialize.version
        fc_nuc_df['centroid_x'], fc_nuc_df['centroid_y'], fc_nuc_df['centroid_z'] = np.stack(fc_nuc_df_orig.centroid.values).T
        return fc_nuc_df
    
    @classmethod
    def fill(cls, client, description=None):
        print('--> Filling FunctionalCoreg table...')
        
        # fetch data from materialization service
        df = cls.fetch_df(client)
        
        print('Inserting data...')
        # insert to main table
        cls.insert(df.to_records(index=False), skip_duplicates=True)
        
        # insert metadata
        cls.Meta.insert1({'ver': client.materialize.version, 'description': description})
        print(f'Successfully inserted functional coregistration for materialization version: {client.materialize.version:.2f}')


class ProofreadSegment(m65mat.ProofreadSegment):

    class Meta(m65mat.ProofreadSegment): pass

    @classmethod
    def fetch_df(cls, client):
        print(f'Fetching data for materialization version {client.materialize.version:.2f}... ')
        
        # fetch dataframes
        fc_df = client.materialize.query_table('proofreading_functional_coreg_v2', filter_out_dict={'pt_root_id': [0]})
        fc_df['ver'] = client.materialize.version
        return fc_df[['ver','pt_root_id']].rename(columns={'pt_root_id':'segment_id'}).drop_duplicates().reset_index(drop=True)

    @classmethod
    def fill(cls, client, description=None):
        print('--> Filling ProofreadSegment table...')
        
        # fetch data from materialization service
        df = cls.fetch_df(client)
        
        print('Inserting data...')
        # insert info to main table
        cls.insert(df.to_records(index=False), skip_duplicates=True)
        
        # insert metadata
        cls.Meta.insert1({'ver': client.materialize.version, 'description': description})
        print(f'Successfully inserted proofread segments for materialization version: {client.materialize.version:.2f}')


class ProofreadFunctionalCoregV2(m65mat.ProofreadFunctionalCoregV2):
        
    class Meta(m65mat.ProofreadFunctionalCoregV2.Meta): pass
    
    @classmethod
    def fetch_df(cls, client):
        print(f'Fetching data for materialization version {client.materialize.version:.2f}... ')
        
        # fetch dataframes
        fc_df = client.materialize.query_table('proofreading_functional_coreg_v2', filter_out_dict={'pt_root_id': [0]})
        nuc_df = client.materialize.query_table('nucleus_detection_v0', filter_out_dict={'pt_root_id': [0]})
        
        # merge proofread and nucleus dataframes
        rename_dict = {
            'id_x': 'nucleus_id', 
            'pt_root_id': 'segment_id', 
            'pt_position_y':'centroid', 
            'pt_supervoxel_id_y': 'supervoxel_id',
            'session': 'scan_session'
        }
        fc_nuc_df_orig = pd.merge(nuc_df, fc_df, on='pt_root_id').rename(columns=rename_dict)
        fc_nuc_df = fc_nuc_df_orig[['nucleus_id', 'segment_id', 'supervoxel_id', 'scan_session', 'scan_idx', 'unit_id']].copy()
        fc_nuc_df['ver'] = client.materialize.version
        fc_nuc_df['centroid_x'], fc_nuc_df['centroid_y'], fc_nuc_df['centroid_z'] = np.stack(fc_nuc_df_orig.centroid.values).T
        return fc_nuc_df


class SynapseSegmentSource(m65mat.SynapseSegmentSource): pass


class Synapse(m65mat.Synapse):
    
    client = None
    
    @property
    def key_source(self):
        return (SynapseSegmentSource & {'include': 1}).proj(primary_seg_id='segment_id')
    
    @classmethod
    def initialize_client(cls, ver='latest'):
        print(f'--> Initializing client...')
        
        client = CAVEclient('minnie65_phase3_v1')
        
        if ver == 'latest':
            ver = client.materialize.most_recent_version()
            print(f'Latest version specified.')
        
        client.materialize._version = ver  
        print(f'Version set to: {ver:.2f}')
              
        cls.client = client
        
        print(f'Client initialized.')
    
    def make(self, key):
        primary_seg_id = np.int(key['primary_seg_id'])
              
        # get synapses where primary segment is presynaptic
        df_pre = self.client.materialize.query_table('synapses_pni_2', filter_equal_dict={'pre_pt_root_id': primary_seg_id})
        df_pre = df_pre.rename(columns={'pre_pt_root_id':'primary_seg_id', 'post_pt_root_id': 'secondary_seg_id'})
        df_pre['prepost'] = 'presyn'
        
        # get synapses where primary segment is postsynaptic
        df_post = self.client.materialize.query_table('synapses_pni_2', filter_equal_dict={'post_pt_root_id': primary_seg_id})
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
            
            final_df = df.rename(columns=rename_dict)[['primary_seg_id', 'secondary_seg_id', 'synapse_id', \
                                                       'prepost', 'synapse_x', 'synapse_y', 'synapse_z', 'synapse_size']]

            self.insert(final_df.to_records(index=False))
        
        else:
            # print(f'No synapses found for primary_seg_id: {primary_seg_id}. Updating SynapseSegmentSource with "include=0".')
            (SynapseSegmentSource & {'segment_id': primary_seg_id})._update('include', 0)


def fetch_materialization(ver=-1, update_latest_tag=True):
    # check that client has an auth token
    try:
        client = CAVEclient('minnie65_phase3_v1')
    except:
        traceback.print_exc()
        client = CAVEclient()
        client.auth.get_new_token()
        token = f"{input(prompt='Paste token (without quotes):')}"
        client.auth.save_token(token=token, overwrite=True)
        print('Checking token...')

        try:
            client = CAVEclient('minnie65_phase3_v1')
        except:
            print('Didnt work. Try running "client.auth.save_token(token="PASTE_TOKEN_HERE", overwrite=True)" inside the notebook.')
            return
    print('Authentication successful.')

    # handle materialization version
    code, client = _version_mgr(client=client, ver=ver)
        
    if code == 0:
        return
    
    print('Fetching materialization...')
    previous_version = (Materialization.CurrentVersion() & {'kind':"latest"}).fetch1('ver')

    try: 
        # run fill method for tables
        Materialization.fill(client=client, ver=ver, update_latest_tag=update_latest_tag)
        Nucleus.fill(client=client)
        Nucleus.fill_synapse_segment_source()
        FunctionalCoreg.fill(client=client)
        ProofreadSegment.fill(client=client)
        ProofreadFunctionalCoregV2.fill(client=client)
        Synapse.initialize_client(ver=client.materialize.version)
        print('--> Filling Synapse table with latest segments from SynapseSegmentSource...')
        Synapse.populate(display_progress=True, suppress_errors=True)
        print('Ensure that no errors occured during Synapse download by checking: "schema.jobs" table')
        print('Done.')

    except:
        print('The materialization download failed with the following error:')
        traceback.print_exc()
        print(f'Resetting "latest version" to previous version: {previous_version:.2f}...')
        (Materialization.CurrentVersion() & {'kind':"latest"})._update('ver', previous_version)
        print(f'Successfully reset "latest version" to {previous_version:.2f}.')
        print('Do you want to delete partial data?')
        (Materialization & {'ver': client.materialize.version}).delete()


## ADDITIONAL TABLES

class AllenV1ColumnTypesSlanted(m65mat.AllenV1ColumnTypesSlanted):
        
    class Meta(m65mat.AllenV1ColumnTypesSlanted.Meta): pass
    
    @classmethod
    def fetch_df(cls, client):
        print(f'Fetching data for materialization version {client.materialize.version:.2f}... ')
        
        # fetch dataframes
        nuc_df = client.materialize.query_table('nucleus_detection_v0', split_positions=True, filter_out_dict={'pt_root_id': [0]})
        col_df = client.materialize.query_table('allen_v1_column_types_slanted', split_positions=True, filter_out_dict={'pt_root_id': [0]})
        merge_df = col_df.merge(nuc_df, on=['pt_root_id', 'valid'])

        # merge proofread and nucleus dataframes
        rename_dict = {
            'id_y': 'nucleus_id',
            'pt_root_id': 'segment_id',
            'pt_position_x_y': 'centroid_x',
            'pt_position_y_y': 'centroid_y',
            'pt_position_z_y': 'centroid_z',
            'pt_supervoxel_id_y': 'supervoxel_id'
        }
        merge_df2 = merge_df.merge(merge_df.groupby(['pt_root_id'], as_index=False).nunique()[['pt_root_id', 'id_y']].rename(columns={'id_y': 'n_nuc'}))
        final_df = merge_df2.rename(columns=rename_dict)
        final_df['ver'] = client.materialize.version
        final_df['valid'] = 1*(final_df.valid.values == 't')

        return final_df
    
    @classmethod
    def fill(cls, client, description=None):
        print('--> Filling AllenV1ColumnTypesSlanted table...')
        
        # fetch data from materialization service
        df = cls.fetch_df(client)
        
        print('Inserting data...')
        # insert info to main table
        cls.insert(df.to_records(index=False), skip_duplicates=True, ignore_extra_fields=True)
        
        # insert metadata
        cls.Meta.insert1({'ver': client.materialize.version, 'description': description})
        print(f'Successfully inserted data for materialization version: {client.materialize.version:.2f}')


class AllenSomaCourseCellClassModelV1(m65mat.AllenSomaCourseCellClassModelV1):
        
    class Meta(m65mat.AllenSomaCourseCellClassModelV1.Meta): pass
    
    @classmethod
    def fetch_df(cls, client):
        print(f'Fetching data for materialization version {client.materialize.version:.2f}... ')
        
        # fetch dataframes
        nuc_df = client.materialize.query_table('nucleus_detection_v0', split_positions=True, filter_out_dict={'pt_root_id': [0]})
        col_df = client.materialize.query_table('allen_soma_coarse_cell_class_model_v1', split_positions=True, filter_out_dict={'pt_root_id': [0]})
        merge_df = col_df.merge(nuc_df, on=['pt_root_id', 'valid'])

        # merge proofread and nucleus dataframes
        rename_dict = {
            'id_y': 'nucleus_id',
            'pt_root_id': 'segment_id',
            'pt_position_x_y': 'centroid_x',
            'pt_position_y_y': 'centroid_y',
            'pt_position_z_y': 'centroid_z',
            'pt_supervoxel_id_y': 'supervoxel_id'
        }
        merge_df2 = merge_df.merge(merge_df.groupby(['pt_root_id'], as_index=False).nunique()[['pt_root_id', 'id_y']].rename(columns={'id_y': 'n_nuc'}))
        final_df = merge_df2.rename(columns=rename_dict)
        final_df['ver'] = client.materialize.version
        final_df['valid'] = 1*(final_df.valid.values == 't')

        return final_df
    
    @classmethod
    def fill(cls, client, description=None):
        print('--> Filling AllenSomaCourseCellClassModelV1 table...')
        
        # fetch data from materialization service
        df = cls.fetch_df(client)
        
        print('Inserting data...')
        # insert info to main table
        cls.insert(df.to_records(index=False), skip_duplicates=True, ignore_extra_fields=True)
        
        # insert metadata
        cls.Meta.insert1({'ver': client.materialize.version, 'description': description})
        print(f'Successfully inserted data for materialization version: {client.materialize.version:.2f}')


class AllenSomaCourseCellClassModelV2(m65mat.AllenSomaCourseCellClassModelV2):
        
    class Meta(m65mat.AllenSomaCourseCellClassModelV2.Meta): pass
    
    @classmethod
    def fetch_df(cls, client):
        print(f'Fetching data for materialization version {client.materialize.version:.2f}... ')
        
        # fetch dataframes
        nuc_df = client.materialize.query_table('nucleus_detection_v0', split_positions=True, filter_out_dict={'pt_root_id': [0]})
        col_df = client.materialize.query_table('allen_soma_coarse_cell_class_model_v2', split_positions=True, filter_out_dict={'pt_root_id': [0]})
        merge_df = col_df.merge(nuc_df, on=['pt_root_id', 'valid'])

        # merge proofread and nucleus dataframes
        rename_dict = {
            'id_y': 'nucleus_id',
            'pt_root_id': 'segment_id',
            'pt_position_x_y': 'centroid_x',
            'pt_position_y_y': 'centroid_y',
            'pt_position_z_y': 'centroid_z',
            'pt_supervoxel_id_y': 'supervoxel_id'
        }
        merge_df2 = merge_df.merge(merge_df.groupby(['pt_root_id'], as_index=False).nunique()[['pt_root_id', 'id_y']].rename(columns={'id_y': 'n_nuc'}))
        final_df = merge_df2.rename(columns=rename_dict)
        final_df['ver'] = client.materialize.version
        final_df['valid'] = 1*(final_df.valid.values == 't')

        return final_df
    
    @classmethod
    def fill(cls, client, description=None):
        print('--> Filling AllenSomaCourseCellClassModelV2 table...')
        
        # fetch data from materialization service
        df = cls.fetch_df(client)
        
        print('Inserting data...')
        # insert info to main table
        cls.insert(df.to_records(index=False), skip_duplicates=True, ignore_extra_fields=True)
        
        # insert metadata
        cls.Meta.insert1({'ver': client.materialize.version, 'description': description})
        print(f'Successfully inserted data for materialization version: {client.materialize.version:.2f}')