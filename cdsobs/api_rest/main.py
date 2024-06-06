import os

import uvicorn
from dotenv import load_dotenv

from cdsobs.utils.logutils import get_logger

load_dotenv()
logger = get_logger(__name__)


if __name__ == "__main__":
    logger.info("Running CADS observation catalogue manager app")
    uvicorn.run(
        "cdsobs.api_rest.app:app",
        host="0.0.0.0",
        port=int(os.environ.get("CADS_OBS_APP_PORT", 8000)),
        reload=False,
        workers=4,
    )
