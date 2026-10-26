from pathlib import Path
from dotenv import load_dotenv, set_key, dotenv_values
import os

ENV_PATH = Path(__file__).parent / ".env"

# Load .env if it exists
if ENV_PATH.exists():
    load_dotenv(dotenv_path=ENV_PATH)
else:
    # Create an empty .env file if missing
    ENV_PATH.touch()

# Define the required paths and their default locations
REQUIRED_PATHS = {
    "ROOT_PATH": Path.home() / "PiFitness",
    "IMAGE_PATH": Path.home() / "PiFitness" / "album_art",
    "MODEL_PATH": Path.home() / "PiFitness" / "models",
    "JSON_EXAMPLES": Path.home() / "PiFitness" / "json_examples",
    # Add any additional paths here
}

def ensure_paths(env_path=ENV_PATH, paths_dict=REQUIRED_PATHS):
    """Ensure all required paths exist and are in the .env file."""
    # Load current env values
    current_env = dotenv_values(env_path)

    for key, default_path in paths_dict.items():
        # Use existing .env value if present, else default
        path_str = current_env.get(key, str(default_path))
        path_obj = Path(path_str)

        # Create directory if it doesn't exist
        path_obj.mkdir(parents=True, exist_ok=True)

        # Update .env if missing
        if key not in current_env:
            set_key(env_path, key, str(path_obj))
            print(f"Added missing {key} to .env: {path_obj}")

    print("All required paths ensured.")

# Call this function at app startup
ensure_paths()

# Example usage
ROOT_PATH = Path(os.getenv("ROOT_PATH"))
IMAGE_PATH = Path(os.getenv("IMAGE_PATH"))
MODEL_PATH = Path(os.getenv("MODEL_PATH"))
JSON_EXAMPLES = Path(os.getenv("JSON_EXAMPLES"))