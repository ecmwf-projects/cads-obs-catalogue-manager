import typer
from botocore.exceptions import BotoCoreError
from sqlalchemy.exc import OperationalError

from cdsobs.utils.exceptions import CliException
from cdsobs.utils.logutils import get_logger

logger = get_logger(__name__)


def exception_handler(exception):
    more_info = "Set CLI_DEBUG=true to see the full trace"
    if isinstance(exception, CliException):
        logger.error(f"Error:\\ {exception}")
    elif isinstance(exception, BotoCoreError):
        logger.error(
            f"Error connecting or operating with the storage. {more_info}:\n {exception}."
        )
    elif isinstance(exception, OperationalError):
        logger.error(
            f"Error connecting or querying the database. {more_info}:\n {exception}."
        )
    else:
        logger.error(f"Error: ({type(exception).__name__}: {exception}), {more_info}")


config_yml_typer = typer.Option(
    None,
    "--config",
    "-c",
    help=(
        "Path to the cdsobs_config yml. If not provided, the function will "
        "search for the file $HOME/.cdsobs/cdsobs_config.yml"
    ),
    envvar="CDSOBS_CONFIG",
    show_default=False,
)

PAGE_SIZE = 50

print_format_msg = "Format to display results, either table or json"


def list_parser(arg: str):
    arg = arg.replace(" ", "")
    if not len(arg):
        return []
    else:
        return arg.split(",")
