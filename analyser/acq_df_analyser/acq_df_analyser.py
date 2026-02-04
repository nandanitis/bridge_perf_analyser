import pandas as pd


def normalise_acq_timecreate_and_sort_df(analysis_df,logger):
    #Adding a column perf_start_time
    analysis_df["time_created"] = pd.to_datetime(analysis_df["time_created"]) 
    analysis_df["time_created"] = ( analysis_df.groupby("perf_folder_name")["time_created"] .transform("min") )
    logger.debug("normalise_acq for time_created column completed")

    # Sort by perf_folder_name
    analysis_df = analysis_df.sort_values(by="perf_folder_name")
    logger.debug("Sorting of acq datafram according to time created ")
    return analysis_df


def start_acq_df_analyser(global_acq_df,RUN_OUTPUT_DIR, logger):
    print(global_acq_df.head())
    return