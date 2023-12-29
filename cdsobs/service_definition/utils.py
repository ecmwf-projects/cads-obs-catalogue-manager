from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def custom_assert(cond: bool, error_log: str, prev_errors: bool = False) -> bool:
    """
    Log error without stopping execution.

    Parameters
    ----------
    cond :
      condition we want to assert
    error_log :
      message to log if there are assertion fails
    prev_errors :
      if there are previous errors

    Returns Updated status of errors in the current execution (true if there has been
    any, false otherwise).
    """
    try:
        assert cond
    except AssertionError:
        logger.error(error_log)
    return not cond or prev_errors
