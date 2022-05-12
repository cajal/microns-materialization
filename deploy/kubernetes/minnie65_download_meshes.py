if __name__ == 'main':
    from microns_materialization.minnie_materialization.minnie65_materialization import Queue, download_materialization
    download_materialization(queue=Queue.Materialization, download_meshes=True, download_synapses=False)