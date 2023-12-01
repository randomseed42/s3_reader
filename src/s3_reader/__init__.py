__version__ = "0.0.1"

from .main import main
from .s3_reader import list_keys_by_prefix, list_keys_by_prefixes, download_by_key, download_by_keys

__all__ = [
    "__version__",
    "main",
    "list_keys_by_prefix",
    "list_keys_by_prefixes",
    "download_by_key",
    "download_by_keys",
]
