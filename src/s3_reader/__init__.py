__version__ = "0.0.2"

from .main import main
from .s3_reader import (
    download_by_key, download_by_keys,
    list_keys_by_prefix, list_keys_by_prefixes,
    list_prefixes_by_prefix, list_prefixes_by_prefixes
)

__all__ = [
    "__version__",
    "main",
    "list_prefixes_by_prefix",
    "list_prefixes_by_prefixes",
    "list_keys_by_prefix",
    "list_keys_by_prefixes",
    "download_by_key",
    "download_by_keys",
]
