import hashlib

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()