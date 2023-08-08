import itertools
import logging
import sys
import os
from logging.handlers import RotatingFileHandler


class _ANSITerminal:
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    @classmethod
    def apply(cls, text, *effects):
        """
        Effect should be a ANSI escape sequence or a list of ANSI escape sequences.
        """
        if not cls._supports_color():
            return text

        ENDC = '\033[0m'

        return ''.join(itertools.chain(effects, [text, ENDC]))

    @classmethod
    def _supports_color(cls):
        """
        Copied from https://github.com/django/django/blob/master/django/core/management/color.py

        Return True if the running system's terminal supports color,
        and False otherwise.
        """
        plat = sys.platform
        supported_platform = plat != 'Pocket PC' and (plat != 'win32' or 'ANSICON' in os.environ)

        # isatty is not always implemented, #6223.
        is_a_tty = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
        if not supported_platform or not is_a_tty:
            return False

        return True


class _CustomConsoleFormatter(logging.Formatter):
    def format(self, record):
        if record.levelno == logging.WARNING:
            record.levelname = _ANSITerminal.apply(record.levelname, _ANSITerminal.YELLOW)
        elif record.levelno == logging.ERROR:
            record.levelname = _ANSITerminal.apply(record.levelname, _ANSITerminal.RED)
        elif record.levelno == logging.CRITICAL:
            record.levelname = _ANSITerminal.apply(record.levelname, _ANSITerminal.RED)
        elif record.levelno == logging.INFO:
            record.levelname = _ANSITerminal.apply(record.levelname, _ANSITerminal.GREEN)
        elif record.levelno == logging.DEBUG:
            record.levelname = _ANSITerminal.apply(record.levelname, _ANSITerminal.BLUE)
        return super().format(record)


class BFGLogging:
    """
    Configure custom loggers.
    """
    LOG_FORMAT = '%(levelname)-8s [%(asctime)s] %(name)s:  %(message)s'

    @classmethod
    def setup(cls, log_file=None, add_stdout_handler=True, debug=False):
        root_logger = logging.getLogger()

        if log_file is not None:
            file_formatter = logging.Formatter(cls.LOG_FORMAT)
            file_handler = RotatingFileHandler(log_file, maxBytes=(1048576 * 5), backupCount=3)
            file_handler.setFormatter(file_formatter)
            file_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(file_handler)

        if add_stdout_handler:
            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = _CustomConsoleFormatter(cls.LOG_FORMAT)
            console_handler.setFormatter(console_formatter)
            console_handler.setLevel(logging.DEBUG)
            root_logger.addHandler(console_handler)

        logging_level = 'DEBUG' if debug else 'INFO'
        root_logger.setLevel(logging_level)
