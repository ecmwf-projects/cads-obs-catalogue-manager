from cdsobs.ingestion.notify import notify_to_slack


def test_slack_notify():
    notify_to_slack("make-production run finished")
