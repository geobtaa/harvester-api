#!/usr/bin/env python3
import sys
import re
from datetime import date
from pathlib import Path
import pandas as pd

# ---- Paths ----
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUTS_DIR = SCRIPT_DIR.parent / "outputs"

# Accept yyyy-mm_dd OR yyyy-mm-dd
FNAME_RE = re.compile(r"^(\d{4}-\d{2}[-_]\d{2})_arcgis_primary\.csv$")

def load_csv_norm(path: Path) -> pd.DataFrame:
    """Load a CSV/TSV as strings, normalize whitespace and ID, drop blank/dup IDs."""
    # Try comma first, fall back to tab if we don't get an 'ID' column
    df = pd.read_csv(path, dtype=str, keep_default_na=False).fillna("")
    if "ID" not in df.columns:
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False, sep="\t").fillna("")
        except Exception:
            pass

    if "ID" not in df.columns:
        raise SystemExit(f"Missing 'ID' column in {path.name} (tried comma and tab).")

    # Strip whitespace in all string cells
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].str.strip()

    df["ID"] = df["ID"].astype(str).str.strip()
    df = df[df["ID"] != ""].copy()
    df = df.drop_duplicates(subset=["ID"], keep="first")
    return df

# ---- Discover input files ----
candidates = []
for p in OUTPUTS_DIR.iterdir():
    if not p.is_file():
        continue
    m = FNAME_RE.match(p.name)
    if m:
        iso = m.group(1).replace("_", "-")
        try:
            d = date.fromisoformat(iso)
            candidates.append((d, p))
        except ValueError:
            continue

if len(candidates) < 2:
    found = ", ".join(p.name for _, p in candidates) or "(none)"
    raise SystemExit(
        f"Need at least two matching files to compare.\n"
        f"Looked in: {OUTPUTS_DIR}\n"
        f"Matched pattern: {FNAME_RE.pattern}\n"
        f"Found: {found}"
    )

# Sort oldest -> newest
candidates.sort(key=lambda t: t[0])

# Most recent file -> new_df
new_date, new_path = candidates[-1]
# SECOND MOST RECENT (previous) -> old_df   ✅ this was the bug
old_date, old_path = candidates[-2]

if new_path == old_path:
    raise SystemExit("Internal error: new and old paths resolved to the same file.")

print(f"Newest:         {new_path.name}")
print(f"Second-most-recent: {old_path.name}")

# ---- Load & normalize ----
new_df = load_csv_norm(new_path)
old_df = load_csv_norm(old_path)

# ---- Build output df ----
parts = []

# (A) Always include "Websites" from newest file
if "Resource Class" in new_df.columns:
    websites = new_df[new_df["Resource Class"].str.lower() == "websites"].copy()
    if not websites.empty:
        parts.append(websites)

# (B) Rows present in new_df but NOT in old_df (new additions)
new_only = new_df.merge(
    old_df[["ID"]].drop_duplicates(),
    on="ID",
    how="left",
    indicator=True
)
new_only = new_only[new_only["_merge"] == "left_only"].drop(columns=["_merge"])
print(f"New additions: {len(new_only)}")

if not new_only.empty:
    parts.append(new_only)

# (C) Rows present in old_df but NOT in new_df (deleted -> retire)
old_only = old_df.merge(
    new_df[["ID"]].drop_duplicates(),
    on="ID",
    how="left",
    indicator=True
)
old_only = old_only[old_only["_merge"] == "left_only"].drop(columns=["_merge"]).copy()
print(f"To retire:     {len(old_only)}")

if not old_only.empty:
    if "Publication State" not in old_only.columns:
        old_only["Publication State"] = ""
    if "Date Retired" not in old_only.columns:
        old_only["Date Retired"] = ""
    old_only["Publication State"] = "unpublished"
    old_only["Date Retired"] = date.today().isoformat()
    parts.append(old_only)

# Combine and de-dup by ID (keep first occurrence: Websites/new additions/retirements in that order)
if parts:
    df = pd.concat(parts, ignore_index=True)
    if "ID" in df.columns:
        df = df.drop_duplicates(subset=["ID"], keep="first")
else:
    df = pd.DataFrame(columns=new_df.columns)

# ---- Write output ----
out_path = OUTPUTS_DIR / f"{date.today().isoformat()}_arcgis_primary_upload.csv"
df.to_csv(out_path, index=False, encoding="utf-8")

print(f"Wrote {len(df)} rows to {out_path}")
