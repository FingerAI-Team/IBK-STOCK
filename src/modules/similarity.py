import numpy as np 

def l2_similarity(emb1, emb2):
    return np.linalg.norm(emb1 - emb2)