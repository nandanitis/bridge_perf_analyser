
from pathlib import Path
import tarfile

def discover_html_files_from_folders(PERF_PATH, logger):
    """
    Discover all *bridge.default.html files inside first-level perf-stats-* folders
    directly under PERF_PATH (do not check subfolder names for perf-stats-*).
    """
    html_files = []

    # Iterate over first-level items in PERF_PATH
    for folder in PERF_PATH.iterdir():
        if folder.is_dir() and folder.name.startswith("perf-stats-"):
            # Look only inside this folder for bridge.default.html files
            for html_file in folder.glob("*bridge.default.html"):
                if html_file.is_file():
                    html_files.append(html_file)

    logger.info(f"Total Bridge Default HTML files discovered: {len(html_files)}")
    return html_files




def extract_html_from_tar_file(PERF_PATH, logger):
    """
    Extract all *bridge.default.html files from .tar.gz archives
    into PERF_PATH/bridge-default-html-filess/<perf-folder-name>/ without any subfolders.
    Skip extraction if the folder already exists.
    """
    dest_root = PERF_PATH
    dest_root.mkdir(exist_ok=True)
    total_extracted = 0

    for tar_path in PERF_PATH.rglob("*.tar.gz"):
        perf_folder_name = tar_path.name.replace(".tar.gz", "").replace(".tar", "")  # e.g., perf-stats-20260130-093001
        dest_folder = dest_root / perf_folder_name

        if dest_folder.exists():
            logger.info(f"Skipping {tar_path}, destination folder already exists.")
            continue

        dest_folder.mkdir(parents=True, exist_ok=True)
        logger.info(f"Extracting from {tar_path} to {dest_folder} (flat)")

        try:
            with tarfile.open(tar_path, "r:gz") as tar:
                extracted_count = 0
                for member in tar.getmembers():
                    if member.isfile() and member.name.endswith("bridge.default.html"):
                        # Flatten path: ignore internal folders
                        member.name = Path(member.name).name
                        tar.extract(member, path=dest_folder)
                        extracted_count += 1
                logger.info(f"Extracted {extracted_count} files from {tar_path}")
                total_extracted += extracted_count

        except Exception as e:
            logger.warning(f"Failed to extract {tar_path}: {e}")

    logger.info(f"Total bridge.default.html files extracted: {total_extracted}")


    return 


def unzip_and_discover_html_files(PERF_PATH, logger):
    extract_html_from_tar_file(PERF_PATH, logger)
    return  discover_html_files_from_folders(PERF_PATH,logger)
    