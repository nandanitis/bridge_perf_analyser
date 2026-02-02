"""Library Import """
import pandas as pd
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

""" Importing Functions from other Files"""
from utils.utils import setup_user_logger,setup_debug_logger
from stats.stats_input_handler import get_stat_choice,get_input_for_selected_stat
from stats.stats_parser import fetch_all_html_files
from stats.stats_analyser import analyse_global_df

# ---- Configs --- Constants ---- #
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent   # perf_ssc
PERF_PATH = PROJECT_ROOT / "bridge-default-html-files"

def create_folder_to_save_output(BASE_DIR):

    # Create top-level output directory
    OUTPUT_DIR = BASE_DIR / "output"
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Create timestamped subfolder inside output where we will save the current Outputs
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    RUN_OUTPUT_DIR = OUTPUT_DIR / f"output_{timestamp}"
    RUN_OUTPUT_DIR.mkdir(exist_ok=True)
    print(f"Saving outputs to: {RUN_OUTPUT_DIR}")
    return RUN_OUTPUT_DIR


def main():
    global_df_stats = None  # Master DataFrame to store rows of all tablse. Example "View Stats Averaged over 60 secs" rows from all perf file 
 
    # Create Output directory to save All Output and log files.  
    RUN_OUTPUT_DIR = create_folder_to_save_output(BASE_DIR)

    # Settup  user logger and debug logger file run.log 
    #LOG_FILE = RUN_OUTPUT_DIR / "run.log"
    user_logger=setup_user_logger(RUN_OUTPUT_DIR)
    debug_logger=setup_debug_logger(RUN_OUTPUT_DIR)
    
    """
    Lets Input from the User on which Stats option they want to use. "NFS stats" or "SMB Stats" or "View "Stats"
    We also which Name/id{addiontal}. Example view id or client ip to search in nfs stats..etc
    We will be doing Analysis and Plotting Graph for this below values only.
    """
    selected_stat=get_stat_choice(debug_logger)
    debug_logger.info(f"User wants stats analyser for the table : {selected_stat}")
    stat_identifier = get_input_for_selected_stat(selected_stat, debug_logger)
    debug_logger.info(f"User wants stats analyser for {selected_stat} for the Name/Id : {stat_identifier}")
    
    user_logger.info("Perf stats processing started")
    html_files = []
    for html_file in PERF_PATH.rglob("perf-stats-*/*bridge.default.html"):
        html_files.append(html_file)
    debug_logger.info(f"Total Bridge Default HTML files discovered: {len(html_files)}")

    """ 
    Start parsing each html file to create global data frame which contains 
    all the rows for the stats  which has been eneted by user
    """
    for html_file_path in tqdm(html_files, desc="Processing Perf Stats", unit="file"):
        debug_logger.info(f"Start Parsing the file: {html_file_path}")

        df=fetch_all_html_files(html_file_path,selected_stat, debug_logger)
        if global_df_stats is None:
            global_df_stats = df  
        else:
            global_df_stats = pd.concat([global_df_stats, df], ignore_index=True)
        debug_logger.info(f"Finished Parsing file: {html_file_path}")
    debug_logger.info("Finished Parsing all files ")   

    """
    Perform stat analyser and Start graph plotting
    """
    analyse_global_df(global_df_stats, selected_stat, stat_identifier, RUN_OUTPUT_DIR, debug_logger )
    debug_logger.info("test")

if __name__ == "__main__":
    main()



