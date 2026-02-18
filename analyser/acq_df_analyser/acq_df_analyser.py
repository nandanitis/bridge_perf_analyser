import pandas as pd
from analyser.acq_df_analyser.acq_df_normaliser import acqdf_normaliser
from analyser.acq_df_analyser.acq_df_plotter import acqdf_for_plotting_graphs


# Show all columns without truncation
pd.set_option('display.max_columns', None)   
pd.set_option('display.width', None)         
pd.set_option('display.max_rows', None)      
pd.set_option('display.show_dimensions', False)

 
def normalise_acq_timecreate_and_sort_df(analysis_df,logger):
    #Adding a column perf_start_time
    analysis_df["time_created"] = pd.to_datetime(analysis_df["time_created"]) 
    analysis_df["time_created"] = ( analysis_df.groupby("perf_folder_name")["time_created"] .transform("min") )
    logger.debug("normalise_acq for time_created column completed")


    shortnames = {
    'Busy %':"busy_percent",
    'Executing Slots/Reqs': 'ExecSlots',
    'Reqs Queued': 'Queued',
    'Reqs Completed 5s/30s': 'Completed',
    'Avg Exec Time 5s/30s': 'AvgExecTime_5s/30s',
    'Avg Executing Reqs 5s/30s': 'AvgExecReqs_5s/30s',
    'Avg Slots 5s/30s': 'AvgSlots_5s/30s',
    'Req Exec Time': 'ReqExecTime',
    'Req Slots': 'ReqSlots'
    }

    # Rename columns to short forms
    analysis_df.rename(columns=shortnames, inplace=True)

    # Sort by perf_folder_name
    analysis_df = analysis_df.sort_values(by="perf_folder_name")
    logger.debug("Sorting of acq datafram according to time created ")
    return analysis_df


def print_values_to_output(acq_df, logger):
    # Make a copy
    analysis_df = acq_df.copy()

    # Drop unwanted columns
    analysis_df = analysis_df.drop(columns=['perf_folder_name', 'Queue Id'])

    # Ensure all columns are printed fully
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)

    # Group by 'time_created'
    grouped = analysis_df.groupby('time_created')
    logger.info("\n\n")
    title="Admission Controller Queue Stats Across All Perf Traces"
    logger.info(f"\n{'='*20} {title} {'='*20}")
    # Iterate over groups and print
    for time_val, group in grouped:
        # Get perf_folder_name from original acq_df
        perf_folder = acq_df.loc[acq_df['time_created'] == time_val, 'perf_folder_name'].iloc[0]

        logger.info(f"Time Created: {time_val}")
        logger.info(f"Perf Folder: {perf_folder}")

        logger.info(group.to_string(index=False))  # print without index
        logger.info("\n")  # one line gap between groups

    return


def start_acq_df_analyser(acq_df,RUN_OUTPUT_DIR, logger):
    acq_df=normalise_acq_timecreate_and_sort_df(acq_df,logger)
    acq_df = acq_df[acq_df['Queue Id'] == "External IO"]
    """Save the Outputs to the user output file """
    print_values_to_output(acq_df,logger)
    
    """ Normalise all   rows to string value """
    acq_df=acqdf_normaliser(acq_df,logger)

    """Plot Graphs and Save the Output """
    acqdf_for_plotting_graphs(acq_df, RUN_OUTPUT_DIR, logger)

    logger.info(f"\n Completed Admission Control Queue Stats Analysis and Plotting")
    logger.info(f"\n{'='*120}")
    logger.info(f"\n{'='*120}")
    return


"""Index(['time_created', 'perf_folder_name', 'bridge_node_ip', 'Queue Id',
       'Slots', 'Queuing Delay', 'Busy %', 'Executing Slots/Reqs',
       'Reqs Queued', 'Reqs Completed 5s/30s', 'Avg Exec Time 5s/30s',
       'Avg Executing Reqs 5s/30s', 'Avg Slots 5s/30s', 'Req Exec Time',
       'Req Slots'],
      dtype='str')"""