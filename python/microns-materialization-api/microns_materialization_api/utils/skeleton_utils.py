import numpy as np

def convert_skeleton_to_nodes_edges(
    skeleton,
    verbose = False,):
    """
    from BCelli
    """
    
    all_skeleton_vertices = skeleton.reshape(-1,3)
    unique_rows,indices = np.unique(all_skeleton_vertices,return_inverse=True,axis=0)

    #need to merge unique indices so if within a certain range of each other then merge them together
    reshaped_indices = indices.reshape(-1,2)
    
    return unique_rows,reshaped_indices