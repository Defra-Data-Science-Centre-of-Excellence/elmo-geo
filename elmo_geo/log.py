""" Pretty logger to be imported anywhere."""
import logging
import os
from dataclasses import dataclass
from rich.logging import RichHandler


logging.basicConfig(
    level = logging.INFO,
    format = "%(message)s",
    datefmt = "[%X.%f]",
    handlers = [RichHandler(show_path=False)],  # silenced as noisy and messy
)


def set_log_level(log: logging.Logger, level: str):
    """Set the logger level from the logging module presets
    Parameters:
        log: The logging object
        level: The desired logging level from {DEBUG, INFO, WARN, ERROR, CRITICAL}
    """
    log.setLevel(getattr(logging, level.upper()))


def get_logger() -> logging.Logger:
    """Initialise a basic logger to stdout
    Returns:
        The logger object
    """
    log = logging.getLogger(__name__)
    level = os.environ.get("LOG_LEVEL", "INFO")
    set_log_level(log, level)
    return log


LOG = get_logger()


@dataclass
class DataError(Exception):
    """Error raised when data is not what is expected"""
    msg: str = ""
    def __post_init__(self):
        LOG.error(self.msg)


@dataclass
class PartitionError(Exception):
    """Error raised when dataframe partitions are not as expected"""
    msg: str = ""
    def __post_init__(self):
        LOG.error(self.msg)


logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)  # silence py4j logging mess by setting its log level back to ERROR
