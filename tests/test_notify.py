from unittest.mock import Mock

from cdsobs.ingestion.notify import notify_to_slack
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def mock_post_function(webhook_url, data, headers):
    logger.info(
        f"Would post to {webhook_url} the following data: {data} and headers: "
        f"{headers}"
    )
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"success": True}
    return mock_response


def test_slack_notify():
    notify_to_slack("make-production run finished", post_function=mock_post_function)
