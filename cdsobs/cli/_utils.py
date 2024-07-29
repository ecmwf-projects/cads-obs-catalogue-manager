import typer
from botocore.exceptions import BotoCoreError
from rich.console import Console
from sqlalchemy.exc import OperationalError


class CliException(Exception):
    pass


class ConfigNotFound(CliException):
    def __init__(self, msg="Configuration yaml not found"):
        self.message = msg
        super().__init__(self.message)


def exception_handler(exception):
    more_info = "Set CLI_DEBUG=true to see the full trace"
    err_console = Console(stderr=True)
    if isinstance(exception, CliException):
        err_console.print(f"Error:\\ {exception}")
    elif isinstance(exception, BotoCoreError):
        err_console.print(
            f"Error connecting or operating with the storage. {more_info}:\n {exception}."
        )
    elif isinstance(exception, OperationalError):
        err_console.print(
            f"Error connecting or querying the database. {more_info}:\n {exception}."
        )
    else:
        err_console.print(
            f"Error: ({type(exception).__name__}: {exception}), {more_info}"
        )


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
