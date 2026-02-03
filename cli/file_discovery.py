
def discover_html_files(PERF_PATH,logger):
    html_files = []
    for html_file in PERF_PATH.rglob("perf-stats-*/*bridge.default.html"):
        html_files.append(html_file)
    logger.info(f"Total Bridge Default HTML files discovered: {len(html_files)}")
    return html_files