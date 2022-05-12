import os

if __name__ == 'main':
    from microns_materialization.minnie_materialization.minnie65_materialization import Queue, download_pcg_skeletons
    download_pcg_skeletons(Queue.PCGSkeleton, loglevel=os.getenv('MICRONS_LOGLEVEL'))