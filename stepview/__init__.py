import logging
import os
import warnings
from pathlib import Path


def set_logger_3rd_party_lib(logging_level=logging.CRITICAL):
    if logging_level == logging.CRITICAL:
        warnings.filterwarnings(
            action="ignore", message="unclosed", category=ResourceWarning
        )
    for name in ["boto3", "botocore", "urllib3"]:
        logging.getLogger(name).setLevel(logging_level)


set_logger_3rd_party_lib()

root = logging.getLogger()
log_level = os.environ.get("LOG_LEVEL", "INFO")
logging.basicConfig(level=logging.getLevelName(log_level))
project_name = Path(__file__).parent.stem
logger = logging.getLogger(project_name)
