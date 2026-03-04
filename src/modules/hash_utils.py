import hashlib

def generate_hash(user_id, question, date):
    return hashlib.md5(
        f"{user_id}_{question}_{date}".encode()
    ).hexdigest()