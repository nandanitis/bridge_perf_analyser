# file: analyze_filtered_perf.py
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from utils.utils import make_safe_filename


PERF_METRIC_MAP = { "W_IOPS": "Write IOPS", "W_BW": "Write Bandwidth (MiB/s)", "W_Lat": "Write Latency (µs)", "W_Avg OIO": "Write Avg OIO", "W_Avg Size": "Write Avg Size (KiB)", "W_Rand %": "Write Random (%)", "W_Err": "Write Errors", "R_IOPS": "Read IOPS", "R_BW": "Read Bandwidth (MiB/s)", "R_Lat": "Read Latency (µs)", "R_Avg OIO": "Read Avg OIO", "R_Avg Size": "Read Avg Size (KiB)", "R_Rand %": "Read Random (%)", "R_Err": "Read Errors", "R_Zero %": "Read Zero (%)", "R_Cache %": "Read Cache (%)", "R_Hydra %": "Read Hydra (%)", "R_SSD %": "Read SSD (%)", "R_FAA %": "Read FAA (%)", }

def plot_map_and_save_png(df, col,output_dir_images,logger):
    """
    Plots a time-series line graph for a given performance metric acrossmultiple perf traces and bridge nodes.
    Purpose:
    - Helps visualize how a specific Write/Read performance metric
      (e.g. Bandwidth, IOPS, Latency) varies over time.
    - Each line represents one bridge node.
    - X-axis is the perf trace folder (time-ordered).
    - Y-axis is the selected metric.
    """
    df["time_created"] = pd.to_datetime(df["time_created"])
    df["perf_start_time"] = (
        df.groupby("perf_folder_name")["time_created"]
        .transform("min")
    )

    display_name = PERF_METRIC_MAP.get(col, col)
    logger.debug("Plotting Graph for metric {display_name}")
   
    # Rows = Time stamp of the file, columns = bridge_node_ip, values = metric column
    pivot_df = df.pivot(index="perf_start_time",columns="bridge_node_ip", values=col)
    pivot_df = pivot_df.sort_index()

    # Show only HH:MM on X-axis
    pivot_df.index = pivot_df.index.strftime("%H:%M")

    plt.figure(figsize=(12, 6))

    for node_ip in pivot_df.columns:
        plt.plot(pivot_df.index,pivot_df[node_ip],marker="o",label=node_ip)

    """Labelling the Table """
    title = f"{display_name} across Perf Traces Across Active Bridge Nodes"
    plt.title(title)
    plt.xlabel("Perf Start Time")
    plt.ylabel(display_name)

    ymax = pivot_df.max().max()
    if pd.notna(ymax):
        plt.ylim(0, ymax * 1.1)

    plt.legend(title="Bridge Node IP's")
    plt.grid(True)
    plt.tight_layout()


    # FileName to Save the jpeg in the outputfolder
    safe_title = make_safe_filename(title)

    output_file = output_dir_images / f"{safe_title}.png"
    plt.savefig(output_file, dpi=150, bbox_inches="tight")
    plt.close()

    logger.debug(f"Saved plot image: {output_file}")


def df_for_plotting_graphs(df,selected_stat, RUN_OUTPUT_DIR, logger):

    """We are creating output folder to save the images. 
    If the Stat name is NFS Portal Stats Averaged over 60 secs, we want the folder name to be NFS_Portal_Stats_Averaged_over_60_secs"""
    safe_stat_name = selected_stat.replace(" ", "_")
    output_dir_images = Path(RUN_OUTPUT_DIR) / safe_stat_name
    output_dir_images.mkdir(parents=True, exist_ok=True)

  
    """Data Columns Start only from column 6, lets satrt from column W_IOPS"""
    metric_start_col_name="W_IOPS"
    start_idx = df.columns.get_loc(metric_start_col_name)
    metric_columns = df.columns[start_idx:]

    """Save Each Metrics Output as a seprate Image"""
    for col in metric_columns:
        if col in PERF_METRIC_MAP:
            plot_map_and_save_png(df, col,output_dir_images,logger)
    logger.info("Finished Plotting Graphs and its saved to folder {safe_stat_name}")
