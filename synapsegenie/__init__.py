"""Initialize synapsegenie"""
import logging
from .__version__ import __version__

logging.basicConfig(level=logging.INFO)
logging.getLogger(__name__)
logging.getLogger("keyring").setLevel(logging.WARNING)
