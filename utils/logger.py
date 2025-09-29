import logging
import sys
from pathlib import Path

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[94m',
        'INFO': '\033[92m',
        'WARNING': '\033[93m',
        'ERROR': '\033[91m',
        'CRITICAL': '\033[95m'
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelname, '')
        message = super().format(record)
        return f"{color}{message}{self.RESET}"

def setup_logger():
    logger = logging.getLogger('HH_Parser')
    logger.setLevel(logging.DEBUG)

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console_formatter = ColoredFormatter(
        '[%(asctime)s] %(levelname)-8s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console.setFormatter(console_formatter)

    # File handler
    log_file = Path('logs/app.log')
    log_file.parent.mkdir(exist_ok=True)
    file = logging.FileHandler(log_file)
    file.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file.setFormatter(file_formatter)

    logger.addHandler(console)
    logger.addHandler(file)
    return logger

logger = setup_logger()
