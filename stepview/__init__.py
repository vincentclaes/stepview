import logging
import os
from pathlib import Path

root = logging.getLogger()
log_level = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=logging.getLevelName(log_level))
project_name = Path(__file__).parent.stem
logger = logging.getLogger(project_name)
