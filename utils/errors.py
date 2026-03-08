# utils/errors.py

class SFEBaseError(Exception):
    """Base class for all Salesforce AppExchange Engine errors."""


class ConfigError(SFEBaseError):
    """Configuration-related problems (missing keys, invalid values)."""


class StorageError(SFEBaseError):
    """Issues with APIFY_LOCAL_STORAGE_DIR or file system operations."""


class ExtractionError(SFEBaseError):
    """Issues in parsing/selector failures on pages."""


class NetworkError(SFEBaseError):
    """Timeouts, blocks, or other network-related issues."""
