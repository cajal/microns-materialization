import os

if __name__ == 'main':
    from microns_materialization.minnie_materialization.minnie65_materialization import download_materialization
    download_materialization(ver=int(os.getenv('MICRONS_MAT_VER_TO_DL')), download_meshes=True, download_synapses=False, loglevel=os.getenv('MICRONS_LOGLEVEL'))