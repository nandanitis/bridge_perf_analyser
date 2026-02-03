import re
import pandas as pd
from collections import Counter


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


""" 
def speed_to_decide(series):
    
    Decide dominant bandwidth unit in a column.
    Returns conversion function.
    
    unit_pattern = re.compile(r"\b(Bps|KiBps|MiBps|GiBps)\b")

    counts = Counter()

    for val in series.dropna().astype(str):
        match = unit_pattern.search(val)
        if match:
            counts[match.group(1)] += 1

    if not counts:
        return convert_to_mibps  # safe default

    dominant_unit = counts.most_common(1)[0][0]

    return {
        "Bps": convert_to_mibps,
        "KiBps": convert_to_mibps,
        "MiBps": convert_to_mibps,
        "GiBps": convert_to_mibps,
    }[dominant_unit]
"""


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


# Needs testing
def latency_to_ms(value):
    if value in (None, "", "nan"):
        return None

    value = str(value).strip()

    # If already numeric → assume ms
    if re.fullmatch(r"[\d\.]+", value):
        return float(value)

    # Find all number + unit pairs
    matches = re.findall(r"([\d\.]+)\s*(us|ms|s)", value, re.IGNORECASE)
    if not matches:
        return None

    total_ms = 0.0

    for num, unit in matches:
        num = float(num)
        unit = unit.lower()

        if unit == "us":
            total_ms += num / 1000
        elif unit == "ms":
            total_ms += num
        elif unit == "s":
            total_ms += num * 1000

    return total_ms


# Needs Tetsing
def normalize_perf_metric_values(analysis_df):
    df=analysis_df.copy()
    
    #This needs work, we need to standardise throughput to mbps and filesize to bytes/KBs
   
    """We are converting all Write/Read performance metric columns to numeric.
    - Targets columns starting with W_ or R_
    - Strips units like MiBps, us, %, etc.
    - Converts values to float """
    for col in df.columns:
        # Avg I/O size → KiB
        if col in ("W_Avg Size", "R_Avg Size"):
            df[col] = df[col].astype(str).apply(size_to_kib).astype(float)

        # Bandwidth → MiB/s
        elif col in ("W_BW", "R_BW"):
            df[col] = df[col].astype(str).apply(speed_to_mibps).astype(float)

        # Latency → ms
        elif col in ("W_Lat", "R_Lat"):
            df[col] = df[col].astype(str).apply(latency_to_ms).astype(float)

        # Generic numeric cleanup for other W_/R_ columns
        elif col.startswith(("W_", "R_")):
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[^\d\.]", "", regex=True)
                .replace("", None)
                .astype(float)
            )
    return df

