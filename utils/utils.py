import logging
import re
import argparse
import shutil
from pathlib import Path
import time
from datetime import datetime


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


STAT_MAP_FOR_ARGPARSE = {
    "nfs": "NFS Portal Stats Averaged over 60 secs",
    "smb": "SMB Portal Stats Averaged over 60 secs",
    "s3": "S3 Portal Stats Averaged over 60 secs",
    "view": "View Stats Averaged over 60 secs",
    "journal":"Journal stats Averaged over 60 secs",
    "metadata":"Metadata Journal stats Averaged over 60 secs",
    "disk-controller":"Disk Controller Stats Averaged over 60 secs",
    "disk":"Disk Stats Averaged over 60 secs",
    "icebox":"Icebox Vault Node Stats Averaged over 60 secs",
}

STAT_OPTIONS = {
    1: "NFS Portal Stats",
    2: "SMB Portal Stats",
    3: "S3 Portal Stats",
    4: "View Stats",
    5: "Journal Stats",
    6: "Metadata Journal Stats",
    7: "Disk Controller Stats",
    8: "Disk Stats",
    9: "Icebox Vault Node Stats",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Bridge Perf Analyser"
    )

    parser.add_argument(
        "--stat",
        type=str,
        help="Stat type (nfs, smb, s3, view, disk, etc)"
    )

    parser.add_argument(
        "--id",
        type=str,
        help="Stat identifier (View ID, client IP, bucket name, etc)"
    )

    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Run without interactive prompts"
    )

    return parser.parse_args()


def create_folder_to_save_output(BASE_DIR):

    # Create top-level output directory
    OUTPUT_DIR = BASE_DIR / "output"
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Create timestamped subfolder inside output where we will save the current Outputs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RUN_OUTPUT_DIR = OUTPUT_DIR / f"output_{timestamp}"
    RUN_OUTPUT_DIR.mkdir(exist_ok=True)
    #logger.info(f"Saving outputs to: {RUN_OUTPUT_DIR}")
    return RUN_OUTPUT_DIR



def zip_run_output(run_output_dir: str) -> str:
    """
    Zips the RUN_OUTPUT_DIR and returns the zip file path
    """
    run_dir = Path(run_output_dir).resolve()
    zip_path = run_dir.parent / run_dir.name  # without .zip

    shutil.make_archive(
        base_name=str(zip_path),
        format="zip",
        root_dir=run_dir.parent,
        base_dir=run_dir.name
    )

    return f"{zip_path}.zip"

def zip_output_folder(RUN_OUTPUT_DIR,logger):
    zip_file = zip_run_output(RUN_OUTPUT_DIR)
    logger.info(f"\nOutput archived at: {zip_file}")
    logger.info(f"scp user@host:{zip_file} .")