# pipeline/calculations_2.py
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from functools import reduce

# ──────────────────────────────────────────────────────────────────────────
# 1. Locate the latest Excel inside ./Output
# ──────────────────────────────────────────────────────────────────────────
def build_quarter_metrics(path: str):
    df = pd.read_excel(path)
    # … existing logic …
        # ──────────────────────────────────────────────────────────────────────────
# 2. Create FY / FYQ (Indian fiscal Apr-Mar)
# ──────────────────────────────────────────────────────────────────────────
    df["Month"] = pd.to_datetime(df["Month"])

    def fy_label(d):
        return f"FY{(d.year + 1 if d.month >= 4 else d.year) % 100:02d}"

    def fy_quarter(d):
        # Apr-Jun = Q1, Jul-Sep = Q2, Oct-Dec = Q3, Jan-Mar = Q4
        if   d.month in (4, 5, 6):   return "Q1"
        elif d.month in (7, 8, 9):   return "Q2"
        elif d.month in (10, 11, 12):return "Q3"
        else:                        return "Q4"

    df["FY"]   = df["Month"].apply(fy_label)
    df["Q"]    = df["Month"].apply(fy_quarter)
    df["FYQ"]  = df["FY"] + " " + df["Q"]

    # The most-recent FYQ (used to tag “New”)
    latest_fyq = df.loc[df["Month"].idxmax(), "FYQ"]
    print("✨ Latest FY-Quarter in data:", latest_fyq)

    # ──────────────────────────────────────────────────────────────────────────
    # 3. Metrics to summarise
    # ──────────────────────────────────────────────────────────────────────────
    metrics = [
        "TOFU (in lacs)", "BOFU (in lacs)", "Credit Period",
        "Effective Discount (in lacs)", "Days Advanced",
        "Max Days Advanced", "APR", "Buyer Revenue Share"
    ]

    # ──────────────────────────────────────────────────────────────────────────
    # 4. Pivot each metric by FYQ
    # ──────────────────────────────────────────────────────────────────────────
    pivots = []
    for m in metrics:
        piv = (
            df.pivot_table(index=["PAN", "Supplier Name", "Buyer Name"],
                        columns="FYQ", values=m, aggfunc="sum")
            .sort_index(axis=1)                 # FYQ chronological
            .add_prefix(f"{m}__")               # keep metric in col name
            .reset_index()
        )
        pivots.append(piv)

    # Combine all pivots
    merged = reduce(lambda l, r: pd.merge(l, r, on=["PAN", "Supplier Name", "Buyer Name"], how="outer"),
                    pivots)

    # ──────────────────────────────────────────────────────────────────────────
    # 5. First / Last quarter, counts, acceleration
    # ──────────────────────────────────────────────────────────────────────────
    last_month = df["Month"].max()
    cuts = {"18": last_month - relativedelta(months=18),
            "12": last_month - relativedelta(months=12),
            "6":  last_month - relativedelta(months=6)}

    # helper: add first/last quarter & counts
    for m in ["TOFU (in lacs)", "BOFU (in lacs)"]:
        q_cols = merged.filter(like=f"{m}__").columns

        merged[f"First {m} Quarter"] = merged[q_cols].apply(
            lambda r: next((c.split("__")[1] for c in q_cols if pd.notna(r[c]) and r[c]!=0), pd.NA), axis=1)
        merged[f"Last {m} Quarter"] = merged[q_cols].apply(
            lambda r: next((c.split("__")[1] for c in reversed(q_cols) if pd.notna(r[c]) and r[c]!=0), pd.NA), axis=1)

        # TOFU counts
        if m == "TOFU (in lacs)":
            for lbl, cutoff in cuts.items():
                cnt = (df[df["Month"] >= cutoff]
                    .groupby(["PAN", "Supplier Name", "Buyer Name"])[m]
                    .apply(lambda s: (s!=0).sum())
                    .rename(f"{m} count {lbl} month"))
                merged = merged.merge(cnt.reset_index(), how="left",
                                    on=["PAN", "Supplier Name", "Buyer Name"])

        # BOFU / TOFU acceleration
        if m == "BOFU (in lacs)":
            for lbl, cutoff in cuts.items():
                tofu = (df[df["Month"] >= cutoff]
                        .groupby(["PAN", "Supplier Name", "Buyer Name"])["TOFU (in lacs)"].sum())
                bofu = (df[df["Month"] >= cutoff]
                        .groupby(["PAN", "Supplier Name", "Buyer Name"])["BOFU (in lacs)"].sum())
                acc  = (bofu / tofu).replace([float("inf"), -float("inf")], 0).fillna(0)
                merged = merged.merge(acc.rename(f"Acc Rate {lbl} month").reset_index(),
                                    how="left",
                                    on=["PAN", "Supplier Name", "Buyer Name"])

    # ──────────────────────────────────────────────────────────────────────────
    # 6. Dynamic categorisation using latest FYQ
    # ──────────────────────────────────────────────────────────────────────────
    def tofu_cat(first_qtr, cnt, threshold_hi, threshold_mid):
        if pd.isna(first_qtr):
            return "TOFU Low"
        if first_qtr == latest_fyq:
            return "TOFU New"
        if cnt >= threshold_hi:
            return "TOFU Regular"
        if cnt >= threshold_mid:
            return "TOFU Medium"
        return "TOFU Low"

    def bofu_cat(first_qtr, acc, hi=0.8, mid=0.5):
        if pd.isna(first_qtr):
            return "Never Transacted"
        if first_qtr == latest_fyq:
            return "Txn New"
        if acc >= hi:
            return "Txn High"
        if acc >= mid:
            return "Txn Med"
        if acc > 0:
            return "Txn Low"
        return "Never Transacted"


    merged["TOFU Category_18M"] = merged.apply(
        lambda r: tofu_cat(r["First TOFU (in lacs) Quarter"], r.get("TOFU (in lacs) count 18 month", 0), 13, 7), axis=1)
    merged["TOFU Category_12M"] = merged.apply(
        lambda r: tofu_cat(r["First TOFU (in lacs) Quarter"], r.get("TOFU (in lacs) count 12 month", 0), 9, 5), axis=1)
    merged["TOFU Category_6M"]  = merged.apply(
        lambda r: tofu_cat(r["First TOFU (in lacs) Quarter"], r.get("TOFU (in lacs) count 6 month", 0), 5, 3), axis=1)

    merged["BOFU Category_18M"] = merged.apply(
        lambda r: bofu_cat(r["First BOFU (in lacs) Quarter"], r.get("Acc Rate 18 month", 0)), axis=1)
    merged["BOFU Category_12M"] = merged.apply(
        lambda r: bofu_cat(r["First BOFU (in lacs) Quarter"], r.get("Acc Rate 12 month", 0)), axis=1)
    merged["BOFU Category_6M"]  = merged.apply(
        lambda r: bofu_cat(r["First BOFU (in lacs) Quarter"], r.get("Acc Rate 6 month", 0)), axis=1)

    # ──────────────────────────────────────────────────────────────────────────
    # 7. Write sheet “Quarterly Metrics”
    # ──────────────────────────────────────────────────────────────────────────

    with pd.ExcelWriter(path, engine="openpyxl", mode="a",
                        if_sheet_exists="replace") as w:
        merged.to_excel(w, sheet_name="Quaterly Metrics", index=False)


# with pd.ExcelWriter(file_path, engine="openpyxl", mode="a", if_sheet_exists="replace") as writer:
#     merged.to_excel(writer, sheet_name="Quarterly Metrics", index=False)

# print("✅  Sheet 'Quarterly Metrics' added to:", file_path)
