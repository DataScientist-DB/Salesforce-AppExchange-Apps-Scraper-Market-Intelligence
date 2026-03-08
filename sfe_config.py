# sfe_config.py
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from logger import get_logger
from utils.errors import ConfigError

log = get_logger(__name__)

# 🔹 NEW: Root of the Salesforce AppExchange Engine project
PROJECT_ROOT = Path(__file__).resolve().parent


def _load_json_file(path: Path) -> Dict[str, Any]:
    """Load JSON from a file if it exists, else {}."""
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        raise ConfigError(f"Failed to load JSON config from {path}") from e


def _load_env_overrides() -> Dict[str, Any]:
    """
    Load overrides from environment variables with 'SFE_' prefix.

    Example:
      SFE_MODE=apps -> {"mode": "apps"}
      SFE_MAX_PAGES=5 -> {"max_pages": 5}
    """
    env_overrides: Dict[str, Any] = {}
    for key, value in os.environ.items():
        if not key.startswith("SFE_"):
            continue

        short_key = key[4:].lower()
        # Try int
        if value.isdigit():
            env_overrides[short_key] = int(value)
        else:
            env_overrides[short_key] = value

    return env_overrides
# sfe_config.py

CATEGORY_PRESETS = {
    "finance": [
        "https://appexchange.salesforce.com/results?type=App&category=Finance"
    ],
    "human_resources": [
        "https://appexchange.salesforce.com/results?type=App&category=Human%20Resources"
    ],
    "enterprise_resource_planning": [
        "https://appexchange.salesforce.com/results?type=App&category=ERP"
    ],
    "sales": [
        "https://appexchange.salesforce.com/results?type=App&category=Sales"
    ],
    "customer_service": [
        "https://appexchange.salesforce.com/results?type=App&category=Customer%20Service"
    ],
    "it_admin": [
        "https://appexchange.salesforce.com/results?type=App&category=IT%20and%20Admin"
    ],
    "marketing": [
        "https://appexchange.salesforce.com/results?type=App&category=Marketing"
    ],
    "integration": [
        "https://appexchange.salesforce.com/results?type=App&category=Integration"
    ],
    "analytics": [
        "https://appexchange.salesforce.com/results?type=App&category=Analytics"
    ],
    "salesforce_labs": [
        "https://appexchange.salesforce.com/results?type=App&category=Salesforce%20Labs"
    ],
}


def build_config(project_root: str, actor_input: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Final config resolution order (later overwrites earlier):
    1. input.json in project_root (local default)
    2. actor_input (from Apify Actor.get_input())
    3. Environment variables (SFE_*)

    Returns the merged config dict.
    """
    project_root_path = Path(project_root)
    local_input_path = project_root_path / "input.json"

    config: Dict[str, Any] = {}

    # 1. local input.json
    local_cfg = _load_json_file(local_input_path)
    if local_cfg:
        log.info("Loaded config from %s", local_input_path)
        config.update(local_cfg)

    # 2. actor input
    if actor_input:
        log.info("Merging config from Actor INPUT (keys: %s)", list(actor_input.keys()))
        config.update(actor_input)

    # 3. SFE_* env overrides
    env_cfg = _load_env_overrides()
    if env_cfg:
        log.info("Loaded config overrides from env: %s", list(env_cfg.keys()))
        config.update(env_cfg)

    if not config:
        raise ConfigError(
            "No configuration found. Provide input.json, Apify INPUT, or SFE_* env variables."
        )

    return config
