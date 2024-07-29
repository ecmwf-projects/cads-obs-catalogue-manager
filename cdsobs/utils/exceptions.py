class SizeError(Exception):
    pass


class ConfigError(Exception):
    pass


class CatalogueException(Exception):
    pass


class DataNotFoundException(RuntimeError):
    pass


class CliException(Exception):
    pass


class ConfigNotFound(CliException):
    def __init__(self, msg="Configuration yaml not found"):
        self.message = msg
        super().__init__(self.message)
