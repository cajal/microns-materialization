import os

if __name__ == 'main':
    from microns_materialization.minnie_materialization.minnie65_materialization import Queue, download_meshwork_objects
    download_meshwork_objects(Queue.PCGMeshwork, loglevel=os.getenv('MICRONS_LOGLEVEL'))