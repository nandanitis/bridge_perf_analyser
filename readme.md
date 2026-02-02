# Bridge Perf Analyser

## Overview
Tool to parse, analyze, and plot Cohesity Bridge perf stats.

## Features
- Multiple stat types (NFS / SMB / View / etc.)
- Dynamic filtering by Name/Id
- Automatic removal of inactive bridge nodes
- Time-based plotting

## TODO / Roadmap
- [n]  Package using pyproject.toml // having vs code bug, skipping it for now
- [ ] Push to GitHub
- [x] Fix plotting X-axis
- [x] Dual logging (user_output.log, display.log)
- [ ] Change logging where is needed to user. 
- [ ] Save analysis output to text file
- [ ] Add argparse-based CLI
- [ ] Improve Name/Id matching with user selection
