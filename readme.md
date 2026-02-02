# Bridge Perf Analyser

## Overview
Tool to parse, analyze, and plot Cohesity Bridge perf stats.

## Features
- Multiple stat types (NFS / SMB / View / etc.)
- Dynamic filtering by Name/Id
- Automatic removal of inactive bridge nodes
- Time-based plotting

## TODO / Roadmap
- [ ]  Package using pyproject.toml // having vs code bug, skipping it for now
- [x] Handle all changes from GitHub moving forward
- [x] Fix plotting in X-axis
- [x] Create logic Drop bridge nodes where the value is 0 for all metrics across all perf_traces
- [x] Change logging logic to user loging and debug logging . 
- [x] Create tabular analysis output to text file
- [x] Add argparse-based CLI -> First half Completed
- [ ] Improve Name/Id matching with user selection
- [ ] Fix Normalize_perf_metric_columns and make sure edge case are handled properly
- [ ] Add "External IO" Stats Parser
- [ ] Add "External IO" stats Analyser
- [ ] Check if there is a need of External IO plotting and plot the graphs
- [x] Argparse helper option to show command options