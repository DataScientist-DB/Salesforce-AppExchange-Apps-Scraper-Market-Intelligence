# utils/storage.py
import os
from pathlib import Path

from logger import get_logger
from utils.errors import StorageError

log = get_logger(__name__)


def is_running_on_apify() -> bool:
    """
    Lightweight detection.
    Apify sets APIFY_IS_AT_HOME=1 inside platform runs.
    """
    return os.getenv("APIFY_IS_AT_HOME") == "1"


def ensure_apify_local_storage(project_root: str) -> Path:
    """
    Ensure APIFY_LOCAL_STORAGE_DIR is set and exists.

    - For local runs: defaults to <project_root>/apify_storage
    - For Apify Cloud: respects whatever is pre-set
    - Always logs final path
    """
    if is_running_on_apify():
        # Apify will manage APIFY_LOCAL_STORAGE_DIR internally.
        storage_dir = os.getenv("APIFY_LOCAL_STORAGE_DIR")
        if not storage_dir:
            # This shouldn't normally happen, but be defensive.
            storage_dir = "/tmp/apify_storage"
            os.environ["APIFY_LOCAL_STORAGE_DIR"] = storage_dir
            log.warning(
                "APIFY_LOCAL_STORAGE_DIR not set in cloud, defaulting to %s", storage_dir
            )
    else:
        # Local environment
        storage_dir = os.getenv("APIFY_LOCAL_STORAGE_DIR")
        if not storage_dir:
            storage_dir = str(Path(project_root) / "apify_storage")
            os.environ["APIFY_LOCAL_STORAGE_DIR"] = storage_dir
            log.info("APIFY_LOCAL_STORAGE_DIR not set, using local %s", storage_dir)

        # Prevent dataset purge for local runs unless user explicitly overrides
        if not os.getenv("APIFY_DISABLE_DATASET_PURGE"):
            os.environ["APIFY_DISABLE_DATASET_PURGE"] = "1"
            log.info("APIFY_DISABLE_DATASET_PURGE=1 (local runs will preserve datasets)")

    storage_path = Path(storage_dir)
    try:
        storage_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        raise StorageError(f"Failed to create APIFY_LOCAL_STORAGE_DIR at {storage_path}") from e

    log.info("Using APIFY_LOCAL_STORAGE_DIR: %s", storage_path)
    return storage_path
