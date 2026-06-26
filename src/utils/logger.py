import logging
import sys

def get_logger(module_name: str) -> logging.Logger:

    logger = logging.getLogger(module_name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        logging.getLogger("py4j").setLevel(logging.WARNING)

    return logger