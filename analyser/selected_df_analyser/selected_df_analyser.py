from tabulate import tabulate
import time
import pandas as pd
from analyser.selected_df_analyser.selected_df_plotter import df_for_plotting_graphs
from .selected_df_normaliser import normalize_perf_metric_values
import re
import sys


# Needs Testing : 
def find_inactive_bridge_nodes(df, logger, metric_start_col_name="W_IOPS"):
   
    """Find bridge nodes where value is 0 in all metric across all perf traces across all
    In oracle and SQl view dump we will use more parallel bridge Nodes
    If we are doing physical backup or oracle clone with just one bridge node, lets drop the other bridge node

    Returns:
        List[str]: inactive bridge_node_ip values""" 
    
    if metric_start_col_name not in df.columns:
        logger.warning(
            "Metric start column '%s' not found. Cannot detect inactive bridge nodes.",
            metric_start_col_name
        )
        return []

    start_idx = df.columns.get_loc(metric_start_col_name)
    metric_cols = df.columns[start_idx:]

    inactive_nodes_mask = (
        df.groupby("bridge_node_ip")[metric_cols]
          .sum()
          .eq(0)
          .all(axis=1)
    )

    inactive_node_ips = inactive_nodes_mask[inactive_nodes_mask].index.tolist()

    logger.debug(
        "Inactive bridge node detection completed. Found %d inactive node(s).",
        len(inactive_node_ips)
    )

    return inactive_node_ips


# Needs Testing : 
def drop_inactive_bridge_nodes(df, inactive_node_ips, logger):
    """
    Drop rows corresponding to inactive bridge_node_ip values.

    Returns:
        pd.DataFrame: filtered dataframe
    """
    if not inactive_node_ips:
        logger.debug("No inactive bridge nodes to drop.")
        return df

    logger.debug(
        "Dropping inactive bridge_node_ip(s): %s",
        inactive_node_ips
    )

    return df[~df["bridge_node_ip"].isin(inactive_node_ips)]


def normalise_folder_time_and_sort_df(analysis_df,logger):
    #Adding a column perf_start_time
    analysis_df["time_created"] = pd.to_datetime(analysis_df["time_created"]) 
    analysis_df["time_created"] = ( analysis_df.groupby("perf_folder_name")["time_created"] .transform("min") )

    # Sort by perf_folder_name
    analysis_df = analysis_df.sort_values(by="perf_folder_name")
    return analysis_df


def tabular_data_of_the_stat(df,selected_stat,logger, sleep_sec=0.35):
    logger.info(f" Sorting perf trace by time creation")
    time.sleep(3)
    df["time_created"] = pd.to_datetime(df["time_created"])

    base_cols = ["bridge_node_ip", "Total/IOs"]

    # Identify W_* and R_* columns dynamically
    w_cols = [c for c in df.columns if c.startswith("W_")]
    r_cols = [c for c in df.columns if c.startswith("R_")]

    def print_grouped_table(title, metric_cols):
        logger.info(f"\n{'='*20} {selected_stat} {title} {'='*20}")

        cols_to_show = base_cols + metric_cols

        for time_val, group in df.groupby("time_created", sort=True):
            logger.info(f"\n{time_val}")
            logger.info(
                group[cols_to_show].to_string(
                    index=False,
                    line_width=180,
                    max_colwidth=14
                )
            )
            time.sleep(sleep_sec)

    # WRITE metrics table
    print_grouped_table("WRITE METRICS (W_*)", w_cols)

    # READ metrics table
    print_grouped_table("READ METRICS (R_*)", r_cols)
    return


def filter_only_unique_name_id(df,stat_identifier,logger):
    """
    Handle Name/Id selection logic:
    - 0 unique  -> log error and exit
    - 1 unique  -> return it
    - >1 unique -> prompt user to pick until valid
    """

    stat_identifier = "10907017373"
    pattern = rf"^{re.escape(stat_identifier)}"
    matched_rows = df[df["Name/Id"].astype(str).str.contains(pattern)]
    unique_vals = matched_rows["Name/Id"].dropna().unique()

    # Case 1: No matches
    if len(unique_vals) == 0:
        logger.error("No matching Name/Id values found. Exiting.......")
        sys.exit(1)

    # Case 2: Exactly one match
    if len(unique_vals) == 1:
        logger.info(f"Name/Id found: {unique_vals[0]}")
        return unique_vals[0]

    # Case 3: Multiple matches → ask user
    logger.info("Multiple Name/Id values found:")
    for idx, val in enumerate(unique_vals, start=1):
        print(f"{idx}. {val}")
    print("0. Exit")

    while True:
        choice = input("Select an option (0 to exit): ").strip()

        if choice == "0":
            logger.info("User chose to exit.")
            sys.exit(0)

        if choice.isdigit():
            choice = int(choice)
            if 1 <= choice <= len(unique_vals):
                selected = unique_vals[choice - 1]
                logger.info(f"User selected Name/Id: {selected}")
                return selected


def analyse_selected_df(global_df_stats, selected_stat, stat_identifier, RUN_OUTPUT_DIR, logger ):
   
    num_rows, num_cols = global_df_stats.shape
    logger.debug("Called analyse_global_df function to Analyse the DF")
    logger.debug(f"Rows: {num_rows}, Columns: {num_cols}")
    logger.debug(f"Column Names For Reference {global_df_stats.columns}")
    
    """ Filter rows where Name/Id column matches the input given by user. Example only for viewId :'10907017373:TestAndDev:6' """
    analysis_df=global_df_stats.copy()
    stat_identifier=filter_only_unique_name_id(analysis_df,stat_identifier,logger)
    analysis_df= analysis_df[analysis_df["Name/Id"] == stat_identifier]
    
    """We need standardise all string values to float so that we can plot the graphs later.For example Kibps,Mibps,Bps to Kibps etc..."""
    plotting_df=analysis_df.copy()
    plotting_df=normalize_perf_metric_values(plotting_df)

    #This will need better implementation or better logic later on
    inactive_node_ips=find_inactive_bridge_nodes(plotting_df, logger,  metric_start_col_name="W_IOPS")
    analysis_df=drop_inactive_bridge_nodes(analysis_df,inactive_node_ips, logger)
    plotting_df=drop_inactive_bridge_nodes(plotting_df,inactive_node_ips, logger)


    """Display The columns and Rows in the UI in a neat  Way """
    analysis_df=normalise_folder_time_and_sort_df(analysis_df,logger)
    tabular_data_of_the_stat(analysis_df,selected_stat,logger)
    

    """Calling the plotting function to plot graphs"""
    df_for_plotting_graphs(plotting_df,selected_stat, RUN_OUTPUT_DIR, logger)
    return
    

"""
Column Names For Refernce: 
Index(['time_created', 'perf_folder_name', 'bridge_node_ip', 'Name/Id',
       'Total/IOs', 'W_IOPS', 'W_BW', 'W_Lat', 'W_Avg OIO', 'W_Avg Size',
       'W_Rand %', 'W_Err', 'R_IOPS', 'R_BW', 'R_Lat', 'R_Avg OIO',
       'R_Avg Size', 'R_Rand %', 'R_Err', 'R_Zero %', 'R_Cache %', 'R_Hydra %',
       'R_SSD %', 'R_FAA %'],
      dtype='str')

""" 
