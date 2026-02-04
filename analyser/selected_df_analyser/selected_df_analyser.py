from tabulate import tabulate
import time
import pandas as pd
from analyser.selected_df_analyser.selected_df_plotter import df_for_plotting_graphs
from .selected_df_normaliser import normalize_perf_metric_values
import re
import sys

# Needs Function Improvement
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
    logger.info("")
    logger.info("Inactive bridge node detection completed. Found %d inactive node(s).",len(inactive_node_ips))
    logger.info("")

    return inactive_node_ips


def drop_inactive_bridge_nodes(df, inactive_node_ips,flag,logger):
    """
    Drop rows corresponding to inactive bridge_node_ip values.
    Returns:
        pd.DataFrame: filtered dataframe
    """
    if flag==1:
        logger.info(f"These below bridge node ips have 0 Read/Write Metrics Across All perf traces. If backup/restore happened through below bridge node, please check why it was inactive")
        logger.info("Inactive bridge_node_ip(s): %s",inactive_node_ips)

    return df[~df["bridge_node_ip"].isin(inactive_node_ips)]


def normalise_folder_time_and_sort_df(analysis_df,logger):
    #Adding a column perf_start_time
    analysis_df["time_created"] = pd.to_datetime(analysis_df["time_created"]) 
    analysis_df["time_created"] = ( analysis_df.groupby("perf_folder_name")["time_created"] .transform("min") )

    # Sort by perf_folder_name
    logger.info(f"Sorting perf traces by folder_name/time_creation")
    analysis_df = analysis_df.sort_values(by="perf_folder_name")
    return analysis_df


def tabular_data_of_the_stat(analysis_df,selected_stat,logger, sleep_sec=0.35):
    df=analysis_df.copy()
    time.sleep(1.3)
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


def print_avg_metrics_per_bridge_node(analysis_df,stat_identifier, logger):
    """
    Prints average read/write metrics per bridge node IP.
    One row per bridge_node_ip.
    """
    df = analysis_df.copy()
    total_perfs=len(df["perf_folder_name"].unique())
    logger.info( f"\n\n{'='*20} Average value of Read/Write Metrics from all {total_perfs} Perf Traces for {stat_identifier}  {'='*20}")
    logger.info("\n")
    logger.info("Below table is average of each Metric across all perf trace. ")
    logger.info("Lets say you have collected 4 perf traces, where bandwdith across the nodes is 10,10,12,0")
    logger.info("Then in the below table the value be (10+12+10+0)/4=4.25 Mbps, we are including 0 as well ")
    logger.info("\n")
    logger.info("*NOTE: There could be instances where the read/write metric could have been gone to zero mulitple times bringing down the average. ")
    logger.info("\tCheck the graph's of the required read/write metric to understad the distrubution of values across all perf traces in a better way ")
    logger.info("\tThats why collecting atleast 10-12 perf traces over 15 minutes interval would give a better average as we will have more data points")
    logger.info("\n")
    if df.empty:
        logger.warning("No data available to compute bridge node averages")
        return

    start_col = "Total/IOs"
    if start_col not in df.columns:
        logger.error(f"Column '{start_col}' not found in DataFrame")
        return

    metric_start_idx = df.columns.get_loc(start_col)
    metric_cols = df.columns[metric_start_idx:]

    avg_df = (df
        .groupby("bridge_node_ip", as_index=False)[metric_cols]
        .mean(numeric_only=True)
    )

    logger.info(
        avg_df.to_string(
            index=False,
            line_width=200,
            max_colwidth=14
        )
    )


def analyse_selected_df(global_df_stats, selected_stat, stat_identifier, RUN_OUTPUT_DIR, logger ):
   
    num_rows, num_cols = global_df_stats.shape
    logger.debug("Called analyse_global_df function to Analyse the DF")
    logger.debug(f"Rows: {num_rows}, Columns: {num_cols}")
    logger.debug(f"Column Names For Reference {global_df_stats.columns}")
    
    """ Filter rows where Name/Id column matches the input given by user. Example only for viewId :'10907017373:TestAndDev:6' """
    analysis_df=global_df_stats.copy()
    stat_identifier=filter_only_unique_name_id(analysis_df,stat_identifier,logger)
    analysis_df= analysis_df[analysis_df["Name/Id"] == stat_identifier]
    
    """Lets have 2 Dataframe, one where all columns are normalised and converted to flot(plotting_df) and the original datafram which can be used for printing purpise(analysis_df)"""
    """We need standardise all string values to float so that we can plot the graphs later.For example Kibps,Mibps,Bps to Kibps etc..."""
    plotting_df=analysis_df.copy()
    plotting_df=normalize_perf_metric_values(plotting_df)

    #This will need better implementation or better logic later on
    """Lets remove inactive bridge node from both the dataframe  """
    inactive_node_ips=find_inactive_bridge_nodes(plotting_df, logger,  metric_start_col_name="W_IOPS")
    no_of_bridge_nodes=len(plotting_df["bridge_node_ip"].unique())
    no_of_bridge_nodes_dropped=len(inactive_node_ips)
    if(no_of_bridge_nodes==no_of_bridge_nodes_dropped):
        logger.info("There are no bridge/nodes on which any read/write opearation took place. Please check if Name of the stat was entered properly")
        return
    elif(no_of_bridge_nodes_dropped>0):
        analysis_df=drop_inactive_bridge_nodes(analysis_df,inactive_node_ips,1,logger)
        plotting_df=drop_inactive_bridge_nodes(plotting_df,inactive_node_ips,2,logger)

    """Display The columns and Rows in the UI in a neat  Way """
    analysis_df=normalise_folder_time_and_sort_df(analysis_df,logger)
    tabular_data_of_the_stat(analysis_df,selected_stat,logger)
    print_avg_metrics_per_bridge_node(plotting_df,stat_identifier, logger)
    

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
