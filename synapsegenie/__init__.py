"""Initialize synapsegenie"""
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__)
logging.getLogger("keyring").setLevel(logging.WARNING)

# Set package version here
__version__ = "0.0.2"

__all__ = ["__version__"]