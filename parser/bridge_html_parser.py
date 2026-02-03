import pandas as pd
from io import StringIO 
from datetime import datetime
from bs4 import BeautifulSoup
from tqdm import tqdm

# Need to add logging and conditions if the column is not found
def flatten_and_shorten_columns(df):
    """
    The column of table is multi header, one level column is write and read and the second level is all the metrics. 
    Dicfficult to work this data, so lets merge them into column of one level
    Flatten multi-level headers in a DataFrame and shorten Write/Read prefixes.
    Example:
        ('Write', 'IOPS')  -> 'W_IOPS'
        ('Write', 'BW')  -> 'W_BW'
        ('Read', 'IOPS')  -> 'R_IOPS'
        ('Read', 'BW')     -> 'R_BW'
        ('Name/Id', 'Name/Id') -> 'Name/Id'
        ('Total IOs', 'Total IOs') -> 'Total IOs'
    """
    # Flatten multi-index columns (if any)
    df.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col 
                  
                  for col in df.columns.values]
    
    # Shorten column names
    def shorten(col):
        if col.startswith("Write_"):
            return col.replace("Write_", "W_")
        elif col.startswith("Read_"):
            return col.replace("Read_", "R_")
        elif col == "Name/Id_Name/Id":
            return "Name/Id"
        elif col == "Total IOs_Total IOs":
            return "Total/IOs"
        else:
            return col
    df.columns = [shorten(col) for col in df.columns]
    return df


def add_metadata_to_table(df, html_file_path):
    """
    Enrich the stats DataFrame with source metadata.These additional columns provide context that is useful for:
    - Time-series analysis
    - Plotting performance metrics across multiple runs

    All added columns are inserted at the beginning of the DataFrame
    So that metadata is clearly visible during inspection and debugging.
    """

    # Use file modified time as a reliable=
    file_mtime = html_file_path.stat().st_mtime
    time_created = datetime.fromtimestamp(file_mtime)

    # Capturing source context from the file path
    perf_folder_name = html_file_path.parent.name
    bridge_node_ip = html_file_path.stem.split(".bridge.default")[0]

    # ---- Add metadata columns to DataFrame ----
    # Insert columns at fixed positions so ordering is consistent
    df.insert(0, "time_created", time_created)
    df.insert(1, "perf_folder_name", perf_folder_name)
    df.insert(2, "bridge_node_ip", bridge_node_ip)

    return df


def find_stat_table_in_html_file(soup, selected_stat, table_offset_index):
    """
    Find a data table in the HTML file based on the stats which we need to check.
    Lets say we want to check stats for View Stats Averaged over 60 secs. But the table for this stats is 2 tables below.
    You will understand it if you see the html file element
    Logic:
    1. Scan all <table> elements. The 
    2. Find the table whose text contains `selected_stat`
       (example: 'View Stats Averaged over 60 secs').
    3. Once found, jump `table_offset_index` tables ahead
       to reach the actual data table. Here table_offset_index would be genrally be 2
    
    NOTE:
    This approach relies on the current HTML layout/order.
    If the HTML structure changes, this logic may break.
    TODO: Improve this by using more stable anchors
          (e.g., surrounding tags, unique patterns, or table structure).
    """

    offset_to_add_after_finding_table_index = table_offset_index

    # Get all tables in the HTML document
    tables = soup.find_all("table")
    marker_index = None

    # Locate the marker table containing the stats name
    for i, table in enumerate(tables):
        if selected_stat in table.get_text(strip=True):
            marker_index = i
            break

    # If marker table is not found, exit early
    if marker_index is None:
        #print(f"Marker table with text '{selected_stat}' not found")
        return None

    # Step 2: Jump forward by the known offset to reach the data table
    data_table_index = marker_index + offset_to_add_after_finding_table_index

    # Boundary check to avoid IndexError
    if data_table_index >= len(tables):
        print("No table found at the expected offset")
        return None

    # Step 3: Return the target stats/data table
    stats_table = tables[data_table_index]
    return stats_table


def fetch_all_html_files(html_file_path,selected_stat,logger):
    #print(f"Please hold on...... Handling Perf stat {html_file_path}")
    soup=None
    with open(html_file_path, "r", encoding="utf-8", errors="ignore") as f:
        soup = BeautifulSoup(f, "html.parser")

    """Below code is to get the dataframe for admission_control_queue_stats
    Admission_control_queue_stats=acq_stats"""
    acq_stat_name="Admission Controller Queue Stats"
    acq_stat_table=find_stat_table_in_html_file(soup,acq_stat_name,1)
    if acq_stat_table is None:
        logger.info(f"{acq_stat_name} table not found in {html_file_path}, skipping...")
        logger.error("Error while fetching the html files, exiting....")
        exit()
    acq_stat_table_html = str(acq_stat_table)

    # Wrap in a StringIO so pandas reads it as HTML content
    acq_df = pd.read_html(StringIO(acq_stat_table_html))[0]
    acq_df = add_metadata_to_table(acq_df,html_file_path)



    """Below Code is the get the dataframe for the stat analyser  which user want to check for """
    user_stat_table=find_stat_table_in_html_file(soup,selected_stat,2)
    if user_stat_table is None:
        logger.info(f"{selected_stat} table not found in {html_file_path}, skipping...")
        exit()
    stat_table_html = str(user_stat_table)
    # Wrap in a StringIO so pandas reads it as HTML content
    df = pd.read_html(StringIO(stat_table_html))[0]
    #Flatten the multi level column to single level column 
    df = flatten_and_shorten_columns(df)
    # Add 3 columns to the current table 
    df = add_metadata_to_table(df,html_file_path)
    return df,acq_df


def build_final_dataframes(html_files,selected_stat, is_bridge_only,logger):
    """ Temporary collectors"""
    acq_dfs = []
    selected_dfs = []

    """Start Parser"""
    for html_file_path in tqdm(html_files, desc="Processing Perf Stats", unit="file"):
        logger.debug(f"Start Parsing file: {html_file_path}")

        selected_df, acq_df = fetch_all_html_files(
            html_file_path,
            selected_stat,
            logger
        )

        # Admission control stats (always collected)
        if acq_df is not None and not acq_df.empty:
            acq_dfs.append(acq_df)

        # User-selected stats (only if not bridge-only)
        if not is_bridge_only:
            if selected_df is not None and not selected_df.empty:
                selected_dfs.append(selected_df)

        logger.debug(f"Finished Parsing file: {html_file_path}")

    logger.info("Finished Parsing all files")

    # Final DataFrames
    global_acq_df = (
        pd.concat(acq_dfs, ignore_index=True)
        if acq_dfs else pd.DataFrame()
    )

    if not is_bridge_only:
        global_selected_df = (
            pd.concat(selected_dfs, ignore_index=True)
            if selected_dfs else pd.DataFrame()
        )
    else:
        global_selected_df = None
    return global_acq_df,global_selected_df