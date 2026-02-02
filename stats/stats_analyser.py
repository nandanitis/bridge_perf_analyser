import re
from plots.plotting_graphs import df_for_plotting_graphs

# Needs Tetsing and error handling, currently converting everything to kib
def size_to_kib(value):
        """
        Convert size strings (B, KiB, MiB, GiB) to KiB.
        """
        if not value or value == "nan":
            return None

        value = value.strip()

        match = re.match(r"([\d\.]+)\s*(B|KiB|MiB|GiB)", value)
        if not match:
            return None

        number, unit = match.groups()
        number = float(number)

        if unit == "B":
            return number / 1024
        elif unit == "KiB":
            return number
        elif unit == "MiB":
            return number * 1024
        elif unit == "GiB":
            return number * 1024 * 1024


# Needs Tetsing and error handling, currentlyconverting everything to to Mibps
def speed_to_mibps(value):
    #Converting Gibps,Bps,Gibps to Kibps
    if value is None or value == "" or value == "nan":
        return None
    value = value.strip()

    # If already numeric, return as float
    if re.fullmatch(r"[\d\.]+", value):
        return float(value)

    match = re.match(r"([\d\.]+)\s*(Bps|KiBps|MiBps|GiBps)", value)
    if not match:
        return None
    num, unit = match.groups()
    num = float(num)

    return {
        "Bps":   num / (1024 * 1024),
        "KiBps": num / 1024,
        "MiBps": num,
        "GiBps": num * 1024,
    }[unit]


# Need to add logic to convert all possible latencies to microseconds or seconds. Currently not implemented
def latency_to_microsec(value):
    return value


# Needs Tetsing
def normalize_perf_metric_columns(df):
    #This needs work, we need to standardise throughput to mbps and filesize to bytes/KBs
   
    """We are converting all Write/Read performance metric columns to numeric.
    - Targets columns starting with W_ or R_
    - Strips units like MiBps, us, %, etc.
    - Converts values to float """
    for col in df.columns:
        #Converting Mibps,Bps,Gibps to Kibps
        if col in ("W_Avg Size", "R_Avg Size"):
            df[col] = df[col].astype(str).apply(size_to_kib).astype(float)

        #Converting Gibps,Bps,Gibps to Kibps
        elif col in ("W_BW", "R_BW"):
            df[col] = df[col].astype(str).apply(speed_to_mibps).astype(float)

        elif col.startswith(("W_", "R_")):
            df[col] = df[col].astype(str).str.replace(r"[^\d\.]", "", regex=True).replace("", None).astype(float)
    return df


# Needs Testing : 
def find_inactive_bridge_nodes(df, logger, metric_start_col_name="W_IOPS"):
    """
    Identify bridge_node_ip values where ALL metric values
    (from metric_start_col_name onward) are zero.

    Returns:
        List[str]: inactive bridge_node_ip values
    """
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


# Main Function of this Page
def analyse_global_df(global_df_stats, selected_stat, stat_identifier, RUN_OUTPUT_DIR, logger ):
    num_rows, num_cols = global_df_stats.shape
    logger.debug(f"Rows: {num_rows}, Columns: {num_cols}")
    logger.debug(f"Column Names {global_df_stats.columns}")
    
    """ Filter rows where Name/Id column matches the input given by user. 
    Example only for view id : '10907017373:TestAndDev:6' """
    analysis_entity_df = global_df_stats[global_df_stats["Name/Id"] == stat_identifier]

    logger.debug(f"Sorting Datframe by perf_folder_name")
    analysis_entity_df = analysis_entity_df.sort_values(by="perf_folder_name")
    
    """output_dir = Path(__file__).resolve().parent
    output_file = output_dir / "filtered_perf_stats.csv"
    analysis_entity_df.to_csv(output_file, index=False) """

    """We need this  to convert all string values to float so that we can plot the graphs later
    For example Kibps,Mibps,Bps to Kibps etc..."""
    plotting_df=normalize_perf_metric_columns(analysis_entity_df)

    """Find bridge nodes where value is 0 in all metric across all perf traces across all
    In oracle and SQl view dump we will use more parallel bridge Nodes
    If we are doing physical backup or oracle clone with just one bridge node, lets drop the other bridge node"""
    #This will need better implementation or better logic
    inactive_node_ips=find_inactive_bridge_nodes(plotting_df, logger,  metric_start_col_name="W_IOPS")

    analysis_df=drop_inactive_bridge_nodes(analysis_entity_df,inactive_node_ips, logger)
    plotting_df=drop_inactive_bridge_nodes(analysis_entity_df,inactive_node_ips, logger)

    logger.info(f"{selected_stat} Sorted by time across all perf traces")
    """Display The columns and Rows in the UI in a neat Tabular Way """

    logger.info("Started plotting the Grap")
    """Calling the plotting function to plot graphs"""
    df_for_plotting_graphs(plotting_df,selected_stat, RUN_OUTPUT_DIR, logger)
    

"""
Column Names: 
Index(['time_created', 'perf_folder_name', 'bridge_node_ip', 'Name/Id',
       'Total/IOs', 'W_IOPS', 'W_BW', 'W_Lat', 'W_Avg OIO', 'W_Avg Size',
       'W_Rand %', 'W_Err', 'R_IOPS', 'R_BW', 'R_Lat', 'R_Avg OIO',
       'R_Avg Size', 'R_Rand %', 'R_Err', 'R_Zero %', 'R_Cache %', 'R_Hydra %',
       'R_SSD %', 'R_FAA %'],
      dtype='str')

""" 
