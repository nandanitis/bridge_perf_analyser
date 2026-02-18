from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from utils.utils import make_safe_filename
import matplotlib.dates as mdates


def acq_plot_map_and_save_png(df, col,output_dir_images,logger):
    """
    Plots a time-series line graph for a given performance metric acrossmultiple perf traces and bridge nodes.
    Purpose:
    - Helps visualize how a specific Write/Read performance metric
      (e.g. Bandwidth, IOPS, Latency) varies over time.
    - Each line represents one bridge node.
    - X-axis is the perf trace folder (time-ordered).
    - Y-axis is the selected metric.
    """
    df["perf_start_time"] = (   df.groupby("perf_folder_name")["time_created"]
        .transform("min")
    )

    #display_name = PERF_METRIC_MAP.get(col, col)
    display_name="Bridge busy percentage"
    logger.debug("Plotting Graph for metric {display_name}")
   
    # Rows = Time stamp of`` the file, columns = bridge_node_ip, values = metric column
    pivot_df = df.pivot(index="perf_start_time",columns="bridge_node_ip", values=col)
    pivot_df = pivot_df.sort_index()

    plt.figure(figsize=(12, 6))

    for node_ip in pivot_df.columns:
        plt.plot(pivot_df.index, pivot_df[node_ip], marker="o", label=node_ip)

    # Format X-axis to show only HH:MM
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
    plt.gca().xaxis.set_major_locator(mdates.AutoDateLocator())

    # Labelling
    title = f"{display_name} across Perf Traces Across Active Bridge Nodes"
    plt.title(title)
    plt.xlabel("Perf Start Time")
    plt.ylabel(display_name)

    # Y-limit only if meaningful
    ymax = pivot_df.max().max()
    if pd.notna(ymax) and ymax > 0:
        plt.ylim(0, ymax * 1.1)

    plt.legend(title="Bridge Node IP's")
    plt.grid(True)
    plt.tight_layout()

    # Save file
    safe_title = make_safe_filename(title)
    output_file = output_dir_images / f"{safe_title}.png"
    plt.savefig(output_file, dpi=150, bbox_inches="tight")
    plt.close()

    logger.debug(f"Saved plot image: {output_file}")
    

def acqdf_for_plotting_graphs(acq_df, RUN_OUTPUT_DIR, logger):
    logger.info("\n")
    logger.info("Started plotting the Graph for Admission Control Queue")
    logger.debug("Called acq_plot_map_and_save_png function")

    """We are creating output folder to save the images"""
    output_folder_name = "admission_control_queue"
    output_dir_images = Path(RUN_OUTPUT_DIR) / output_folder_name
    output_dir_images.mkdir(parents=True, exist_ok=True)

    """Lets Perform it only from bridge busy percent for now, we can add more in the future if needed """
    metric_start_col_name="busy_percent"
    #start_idx = df.columns.get_loc(metric_start_col_name)
    #metric_columns = df.columns[start_idx:]
    metric_columns = [metric_start_col_name]

    """Save Each Column Output as a seprate Image"""
    for col in metric_columns:
            acq_plot_map_and_save_png(acq_df, col,output_dir_images,logger)
    logger.info(f"Finished Plotting Graphs and its saved to folder {output_folder_name}")
    return
