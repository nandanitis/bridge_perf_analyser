"""Library Import """
from pathlib import Path

""" Importing Functions from other Files"""
from utils.utils import setup_logger,create_folder_to_save_output,zip_output_folder
from cli.file_discovery import discover_html_files
from cli.input_handler import collect_run_configuration
from parser.bridge_html_parser import build_final_dataframes
from analyser.acq_df_analyser.acq_df_analyser import start_acq_df_analyser
from analyser.selected_df_analyser.selected_df_analyser import analyse_selected_df


# ---- Configure_Constants ---- #
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent   # perf_ssc
PERF_PATH = PROJECT_ROOT / "bridge-default-html-files"


def main():
    
    """Create Output directory and setup logger for the project"""
    RUN_OUTPUT_DIR = create_folder_to_save_output(BASE_DIR)
    logger=setup_logger(RUN_OUTPUT_DIR)
    
    """Collect which stat graph user needs be analyser(selected_stat) and which name/Id to analyser(stat_identifier) """
    selected_stat,stat_identifier,is_bridge_only=collect_run_configuration(logger)
   
    global_acq_df=None  # Dataframe holds  admission control Queue stats only from all html files
    global_selected_df = None  # DataFrame holds user choosen data tables from all html files
    logger.info(f"Saving outputs to: {RUN_OUTPUT_DIR}")
    logger.info("Perf Analyser Processing Started")

    """Discover all bridge default html files"""
    html_files = discover_html_files(PERF_PATH,logger)

    """Create Dataframe by looping through all bridge defaul files"""
    global_acq_df, global_selected_df = build_final_dataframes(html_files,selected_stat, is_bridge_only,logger)
    
    """Now that we have the dataframe lets start analysing them and plots the graphs"""
    #start_acq_df_analyser(global_acq_df,RUN_OUTPUT_DIR, logger )
    if not is_bridge_only:
        """Perform User choosen analyser """
        analyse_selected_df(global_selected_df, selected_stat, stat_identifier, RUN_OUTPUT_DIR, logger )
        
    """Ziping the folder so that user can scp it to their machine"""
    #zip_output_folder(RUN_OUTPUT_DIR,logger)

if __name__ == "__main__":
    main()

