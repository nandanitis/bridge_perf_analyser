# Bridge Perf Analyser

## Overview
Tool to parse, analyze, and plot Cohesity Bridge perf stats.

## Features
- Multiple stat types (NFS / SMB / View / etc.)
- Dynamic filtering by Name/Id
- Automatic removal of inactive bridge nodes
- Time-based plotting

## TODO / Roadmap
- [ ] Package using pyproject.toml // having vs code bug, skipping it for now
- [x] Handle all changes from GitHub moving forward
- [x] Fix plotting in X-axis
- [x] Create logic Drop bridge nodes where the value is 0 for all metrics across all perf_traces
- [x] Change logging logic to user loging and debug logging . 
- [x] Create tabular analysis output to text file
- [x] Add argparse-based CLI -> First half Completed
- [x] Improve Name/Id matching with user selection
- [x] Argparse helper option to show command options
- [x] Zip the output folder, so that user can scp it to local folder
- [x] Fix latency : Complete function latency_to_ms
- [x] Complete "External IO" Stats Parser
- [ ] Compelte "External IO" stats Analyser
- [ ] Complete External IO plotting and plot the graphs
- [ ] Need to  Improve the grep function to handle the files
- [ ] We need to work on get_input_for_selected_stat function
- [ ] Need to work on  Normalize_perf_metric_columns and make sure different speeds and different file sizes are handeled