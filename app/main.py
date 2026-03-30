import sys
import logging
from app.core.settings import settings
from app.core.server import create_app

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout) # Direct logs to stdout
    ]
)

_log_level = getattr(logging, settings.LOG_LEVEL, logging.INFO)
logger = logging.Logger(
    name="PhotoTransformer",
    level=_log_level
)


app = create_app()
