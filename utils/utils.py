import logging
import re

def make_safe_filename(text: str) -> str:
    """
    Convert a string into a filesystem-safe filename.
    """
    text = text.strip()
    text = re.sub(r"[^\w\-\.]", "_", text)   # replace unsafe chars
    return re.sub(r"_+", "_", text)          # collapse multiple underscores


def setup_user_logger(log_dir):
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("user")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter("%(message)s")

    # File handler
    file_handler = logging.FileHandler(log_dir / "user_output.log")
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


def setup_debug_logger(log_dir):
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("debug")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_dir / "debug.log")
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger



""" 
def setup_logger(log_dir):
    logger = logging.getLogger("perf_stats")
    logger.setLevel(logging.INFO)

    # Prevent duplicate logs if function is called twice
    if logger.handlers:
        return logger

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)

    # Console handler [ Currently its not needed, will add this later if needed]
    #ch = logging.StreamHandler()
    #ch.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    fh.setFormatter(formatter)
    #ch.setFormatter(formatter)

    logger.addHandler(fh)
    #logger.addHandler(ch)

    return logger """ 
