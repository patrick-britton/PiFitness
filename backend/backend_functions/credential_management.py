import json
from cryptography.fernet import Fernet
from dotenv import load_dotenv
import os
from pathlib import Path
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


# credential_management.py lives at <root>/backend/backend_functions/
# .env lives at <root>/backend/.env
load_dotenv(Path(__file__).resolve().parent.parent / ".env")


def load_key():
    # Creates a unique encryption key to be used by the application
    key_path = os.getenv("KEY_PATH")
    if not key_path:
        raise RuntimeError("KEY_PATH is not set — check backend/.env")
    passphrase = Path(key_path).read_text().strip()
    salt = 'static'
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt.encode(),
        iterations=390_000,
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase.encode()))


def encrypt_dict(dict, key=None):
    # Convert dict to JSON string, then encrypt
    if key is None:
        key = load_key()
    json_data = json.dumps(dict)
    return Fernet(key).encrypt(json_data.encode()).decode()



def decrypt_dict(token, key=None):
    if key is None:
        key = load_key()
    decoded_data = Fernet(key).decrypt(token.encode()).decode()
    # Convert back to dict
    return json.loads(decoded_data)