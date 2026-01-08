from pathlib import Path
from typing import Union


def save_artifact(subdir=None, filename=None, content=None, extension=None):
    # saves the specified file locally

    # Ensure directory exists
    project_root = Path(__file__).resolve().parent.parent
    storage_root = project_root / "local_storage"
    target_dir = storage_root / subdir
    target_dir.mkdir(parents=True, exist_ok=True)

    # Create filename
    file_path = target_dir / f"{filename}.{extension}"

    # Write file
    write_mode = "wb" if isinstance(content, bytes) else "w"
    with open(file_path, write_mode, encoding=None if write_mode == "wb" else "utf-8") as f:
        f.write(content)

    return file_path


