from cryptography.fernet import Fernet
from dotenv import load_dotenv
import os
from pathlib import Path
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


load_dotenv()


def load_key():
    # Creates a unique encryption key to be used by the application
    passphrase = Path(os.getenv("KEY_PATH")).read_text().strip()
    salt = 'static'
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt.encode(),
        iterations=390_000,
    )
    return base64.urlsafe_b64encode(kdf.derive(passphrase.encode()))


def encrypt_text(text, key=load_key()):
    return Fernet(key).encrypt(text.encode()).decode()


def decrypt_text(token, key=load_key()):
    return Fernet(key).decrypt(token.encode()).decode()
