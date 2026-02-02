import logging
import re

def make_safe_filename(text: str) -> str:
    """
    Convert a string into a filesystem-safe filename.
    """
    text = text.strip()
    text = re.sub(r"[^\w\-\.]", "_", text)   # replace unsafe chars
    return re.sub(r"_+", "_", text)          # collapse multiple underscores


def setup_logger(log_dir):
    """
    Creates a single application logger with:
    - User-facing logs (INFO+): console + user_output.log (no timestamp)
    - Debug logs (DEBUG+): debug.log only (with timestamp)
    """

    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("perf_analyser")
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers if function is called multiple times
    if logger.handlers:
        return logger

    # -------- User-facing handler (console) --------
    user_console_handler = logging.StreamHandler()
    user_console_handler.setLevel(logging.INFO)
    user_console_handler.setFormatter(
        logging.Formatter("%(message)s")
    )

    # -------- User-facing handler (file) --------
    user_file_handler = logging.FileHandler(log_dir / "user_output.log")
    user_file_handler.setLevel(logging.INFO)
    user_file_handler.setFormatter(
        logging.Formatter("%(message)s")
    )

    # -------- Debug handler (file only) --------
    debug_file_handler = logging.FileHandler(log_dir / "debug.log")
    debug_file_handler.setLevel(logging.DEBUG)
    debug_file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )

    logger.addHandler(user_console_handler)
    logger.addHandler(user_file_handler)
    logger.addHandler(debug_file_handler)

    return logger
