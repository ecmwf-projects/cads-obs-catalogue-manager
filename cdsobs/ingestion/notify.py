import json
import os
from typing import Callable

import requests


def notify_to_slack(message: str, post_function: Callable = requests.post):
    webhook_url = os.getenv("CADSOBS_SLACK_WEBHOOK")
    channel = os.getenv("CADSOBS_SLACK_CHANNEL")
    slack_data = {"username": "cadsobs-bot", "channel": channel, "text": message}
    response = post_function(
        webhook_url,
        data=json.dumps(slack_data),
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()
