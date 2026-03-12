ID2LABEL = {0: "stock", 1: "nstock"}
LABEL2ID = {"stock": 0, "nstock": 1}

def encode_labels(labels):
    return [LABEL2ID[label] for label in labels]

def decode_labels(label_ids):
    return [ID2LABEL[label_id] for label_id in label_ids]