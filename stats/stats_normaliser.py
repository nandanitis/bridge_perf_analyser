import re
import pandas as pd

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
def normalize_perf_metric_values(df):
    
    
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

