import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from functools import reduce
from pandas.tseries.offsets import DateOffset

def build_supplier_pivot(path: str):
    raw = pd.read_excel(path)
    # … existing logic …
    # ── 2. Add FY + FYQ (Indian fiscal Apr-Mar) ───────────────────────────────
    raw["Month"] = pd.to_datetime(raw["Month"])

    def fy_label(d):   return f"FY{(d.year+1 if d.month>=4 else d.year)%100:02d}"
    def fy_quarter(d): return ["Q4","Q1","Q1","Q1","Q2","Q2","Q2","Q3","Q3","Q3","Q4","Q4"][d.month-1]

    raw["FY"]  = raw["Month"].apply(fy_label)
    raw["Q"]   = raw["Month"].apply(fy_quarter)
    raw["FYQ"] = raw["FY"] + " " + raw["Q"]
    latest_fyq = raw.loc[raw["Month"].idxmax(), "FYQ"]
    print("✨ Latest FY-Quarter:", latest_fyq)

    # ── 3. Metrics to summarise ───────────────────────────────────────────────
    metrics = [
        "TOFU (in lacs)", "BOFU (in lacs)", "Credit Period",
        "Effective Discount (in lacs)", "Days Advanced",
        "Max Days Advanced", "APR", "Buyer Revenue Share"
    ]

    # ── 4. Supplier-level aggregation but keep Month & FYQ ────────────────────
    supplier_df = (
        raw.groupby(["PAN", "Supplier Name", "Month", "FYQ"], as_index=False)[metrics]
            .sum()
    )

    # ── 5. Build a pivot per metric (rows = supplier, cols = FYQ) ────────────
    pivots = []
    for m in metrics:
        p = (supplier_df.pivot_table(index=["PAN", "Supplier Name"],
                                    columns="FYQ", values=m, aggfunc="sum")
            .sort_index(axis=1)
            .add_prefix(f"{m}__")
            .reset_index())
        pivots.append(p)

    merged = reduce(lambda l,r: pd.merge(l,r,on=["PAN","Supplier Name"], how="outer"), pivots)

    # ── 6. Counts + acceleration (use original supplier_df) ──────────────────
    last_month = raw["Month"].max()
    cuts = {"18": last_month - relativedelta(months=18),
            "12": last_month - relativedelta(months=12),
            "6":  last_month - relativedelta(months=6)}

    # helper: TOFU counts & BOFU/TOFU acc
    for lbl, cutoff in cuts.items():
        # TOFU non-zero month count
        cnt = (supplier_df[supplier_df["Month"] >= cutoff]
            .assign(non_zero=lambda d: d["TOFU (in lacs)"].ne(0))
            .groupby(["PAN","Supplier Name"])["non_zero"].sum()
            .rename(f"TOFU (in lacs) count {lbl} month"))
        merged = merged.merge(cnt.reset_index(), on=["PAN","Supplier Name"], how="left")

        # Acc rate = BOFU / TOFU for the window
        sums = (supplier_df[supplier_df["Month"] >= cutoff]
                .groupby(["PAN","Supplier Name"])[["BOFU (in lacs)","TOFU (in lacs)"]]
                .sum())
        acc = (sums["BOFU (in lacs)"] / sums["TOFU (in lacs)"]).replace([pd.NA, float("inf")], 0).fillna(0)
        merged = merged.merge(acc.rename(f"Acc Rate {lbl} month").reset_index(),
                            on=["PAN","Supplier Name"], how="left")

    # ── 7. First / last FYQ with data for TOFU & BOFU ─────────────────────────
    for m in ["TOFU (in lacs)", "BOFU (in lacs)"]:
        cols = merged.filter(like=f"{m}__").columns
        merged[f"First {m} Quarter"] = merged[cols].apply(
            lambda r: next((c.split("__")[1] for c in cols if pd.notna(r[c]) and r[c]!=0), pd.NA), axis=1)
        merged[f"Last {m} Quarter"]  = merged[cols].apply(
            lambda r: next((c.split("__")[1] for c in reversed(cols) if pd.notna(r[c]) and r[c]!=0), pd.NA), axis=1)

    # ── 8. Categorisation helpers ────────────────────────────────────────────
    def tofu_cat(first_qtr, cnt, hi, mid):
        """
        Pure TOFU-side tag: Regular / Medium / Low / New.
        (No churn logic here – churn is driven by BOFU behaviour.)
        """
        if pd.isna(first_qtr):
            return "TOFU Low"
        if first_qtr == latest_fyq:
            return "TOFU New"
        if cnt >= hi:
            return "TOFU Regular"
        if cnt >= mid:
            return "TOFU Medium"
        return "TOFU Low"


    # Helper to know “last three TOFU months”
    last_three_tofu = (
        supplier_df[supplier_df["TOFU (in lacs)"] > 0]
        .groupby(["PAN", "Supplier Name"])["Month"]
        .nlargest(3)
        .reset_index(level=2, drop=True)
    )

    def bofu_cat(first_qtr, acc,
                last_bofu_qtr,
                n_tofu_instances,
                last_three_tofu_months):

        # 0) Never transacted
        if pd.isna(first_qtr):
            # Distinguish new-TOFU vs old-TOFU
            if n_tofu_instances and last_three_tofu_months.max() >= last_month - DateOffset(months=8):
                return "Not Txn – New TOFU"
            else:
                return "Not Txn – Old TOFU"

        # 1) BOFU happened in latest FYQ ⇒ New
        if first_qtr == latest_fyq:
            return "Txn New"

        # 2) Normal tiers
        if acc >= 0.8:
            return "Txn High"
        if acc >= 0.5:
            return "Txn Med"
        if acc > 0:
            # --- churn / at-risk logic -------------
            # No BOFU in the last 3 TOFU instances
            if last_bofu_qtr is not None:
                months_since_last_bofu = (last_month - last_bofu_qtr).days / 30
                if months_since_last_bofu > 12:
                    return "Churned >1 yr"
                else:
                    return "Churned <1 yr"

            # If last two TOFU arrivals had zero BOFU
            if n_tofu_instances >= 2 and acc == 0:
                return "At-risk of Churn"

            return "Txn Low"

        # No revenue share at all
        return "Not Txn"


    # -------------------------------------------------------------------------
    # Build lookup tables for churn logic
    # -------------------------------------------------------------------------
    # Last BOFU month per supplier
    last_bofu_month = (
        supplier_df[supplier_df["BOFU (in lacs)"] > 0]
        .groupby(["PAN", "Supplier Name"])["Month"]
        .max()
    )

    # TOFU instance count (all time)
    tofu_instance_count = (
        supplier_df[supplier_df["TOFU (in lacs)"] > 0]
        .groupby(["PAN", "Supplier Name"])["Month"]
        .count()
    )

    # -------------------------------------------------------------------------
    # Apply categorisation
    # -------------------------------------------------------------------------
    merged["TOFU Category_18M"] = merged.apply(
        lambda r: tofu_cat(r["First TOFU (in lacs) Quarter"],
                        r.get("TOFU (in lacs) count 18 month", 0), 13, 7), axis=1)

    merged["TOFU Category_12M"] = merged.apply(
        lambda r: tofu_cat(r["First TOFU (in lacs) Quarter"],
                        r.get("TOFU (in lacs) count 12 month", 0), 9, 5), axis=1)

    merged["TOFU Category_6M"] = merged.apply(
        lambda r: tofu_cat(r["First TOFU (in lacs) Quarter"],
                        r.get("TOFU (in lacs) count 6 month", 0), 5, 3), axis=1)

    merged["BOFU Category_18M"] = merged.apply(
        lambda r: bofu_cat(r["First BOFU (in lacs) Quarter"],
                        r.get("Acc Rate 18 month", 0),
                        last_bofu_month.get((r["PAN"], r["Supplier Name"])),
                        tofu_instance_count.get((r["PAN"], r["Supplier Name"]), 0),
                        last_three_tofu.get((r["PAN"], r["Supplier Name"]))),
        axis=1)

    merged["BOFU Category_12M"] = merged.apply(
        lambda r: bofu_cat(r["First BOFU (in lacs) Quarter"],
                        r.get("Acc Rate 12 month", 0),
                        last_bofu_month.get((r["PAN"], r["Supplier Name"])),
                        tofu_instance_count.get((r["PAN"], r["Supplier Name"]), 0),
                        last_three_tofu.get((r["PAN"], r["Supplier Name"]))),
        axis=1)

    merged["BOFU Category_6M"] = merged.apply(
        lambda r: bofu_cat(r["First BOFU (in lacs) Quarter"],
                        r.get("Acc Rate 6 month", 0),
                        last_bofu_month.get((r["PAN"], r["Supplier Name"])),
                        tofu_instance_count.get((r["PAN"], r["Supplier Name"]), 0),
                        last_three_tofu.get((r["PAN"], r["Supplier Name"]))),
        axis=1)

    with pd.ExcelWriter(path, engine="openpyxl", mode="a",
                        if_sheet_exists="replace") as w:
        merged.to_excel(w, sheet_name="Quaterly Metrics wo duplicates", index=False)
