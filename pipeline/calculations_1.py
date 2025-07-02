
# pipeline/calculations_1.py
import pandas as pd
from functools import reduce
from pathlib import Path

def build_month_pivot(excel_path: str) -> None:
    """
    Reads *excel_path*, creates the month-level “All Metrics Pivot” sheet,
    and writes it back (replacing if it already exists).
    """
    df = pd.read_excel(excel_path)

    metrics = [
        "TOFU (in lacs)", "BOFU (in lacs)", "Buyer Revenue Share",
        "Platform Fee (in lacs)", "Effective Discount (in lacs)",
        "Days Advanced", "Max Days Advanced", "APR",
        "Wtd Credit Period- Calculated", "Wtd Max Days-Calculated",
        "Wtd Act Days-Calculated", "Wtd APR"
    ]

    pivots = []
    for m in metrics:
        p = (df.pivot_table(index=["PAN", "Supplier Name", "Buyer Name"],
                            columns="Month", values=m, aggfunc="sum")
               .add_prefix(f"{m}__")
               .reset_index())
        pivots.append(p)

    merged = reduce(
        lambda l, r: pd.merge(l, r, on=["PAN", "Supplier Name", "Buyer Name"], how="outer"),
        pivots
    )

    # add grand totals
    for m in metrics:
        merged[f"Total Sum of {m}"] = merged.filter(like=f"{m}__").sum(axis=1)

    with pd.ExcelWriter(excel_path, engine="openpyxl", mode="a",
                        if_sheet_exists="replace") as w:
        merged.to_excel(w, sheet_name="All Metrics Pivot", index=False)

    print("✅  All-metrics month pivot written to sheet ‘All Metrics Pivot’.")
