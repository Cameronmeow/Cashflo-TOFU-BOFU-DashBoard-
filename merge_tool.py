import os
from io import BytesIO
from typing import List, Set

import pandas as pd
import streamlit as st

# ──────────────────────────────────────────────────────────────
# 🚀  Auto‑merge HubSpot CSV dumps into a single base Excel file
# ----------------------------------------------------------------
# • Drop all CSV files in the ./Hubspot files folder (hot‑reloads)
# • The uploaded Base Excel must contain a unique **PAN** column
# • PANs are standardised (trim + upper‑case) before merging
# • Only genuinely NEW columns in the CSV files are appended
# • Allows easy handoff to finance / analytics teams as a clean
#   Excel sheet with a consistent schema.
# ----------------------------------------------------------------
# Sharad’s requested input schema (24 columns) — used purely to
# show a reminder; the script will not error if extra columns are
# present or some are missing.
# ----------------------------------------------------------------
EXPECTED_COLS: List[str] = [
    "Month", "PAN", "Supplier Name", "Buyer Name", "Buyer Org ID", "Eligibility",
    "TOFU (in lacs)", "BOFU (in lacs)", "Credit Period", "Max Days Advanced",
    "Days Advanced", "Effective Discount (in lacs)", "Platform Fee (in lacs)",
    "Acc Rate", "RM Name", "APR", "Effective Discount Rate", "Platform Fee Rate",
    "Buyer Revenue Share", "Wtd Credit Period- Calculated", "Wtd Max Days-Calculated",
    "Wtd Act Days-Calculated", "Wtd APR",
]

# Contact‑centric columns we never want in the merged output
UNWANTED_COLS: Set[str] = {
    "First Name", "Last Name", "Last Contacted", "Number of times contacted",
    "Designation of Contact Person", "Title", "Job Title", "KDM", "Contact ID",
}


# ──────────────────────────────────────────────────────────────
# Helpers
# ----------------------------------------------------------------
def _normalise_pan(series: pd.Series) -> pd.Series:
    """Trim whitespace, convert to string, and upper‑case (vectorised)."""
    # Guard against pandas treating extension dtypes weirdly by using .apply
    return series.apply(lambda x: str(x).strip().upper() if pd.notna(x) else x)


def _find_pan_column(df: pd.DataFrame) -> str | None:
    """Return the column name that holds PAN (supports a couple of variants)."""
    for col in ("PAN", "PAN Number"):
        if col in df.columns:
            return col
    return None


# ──────────────────────────────────────────────────────────────
# Main Streamlit page
# ----------------------------------------------------------------

def render_page() -> None:  # noqa: C901 — function intentionally long for clarity
    st.subheader("📁 Auto‑Merge All CSVs into Base Excel")

    default_path = "./Hubspot files"
    merge_folder = st.text_input(
        "📁 Enter the full path to the folder containing CSVs to merge",
        value=default_path,
        help="e.g., ./Hubspot files or C:/Users/Sharad/Documents/MergeFolder"
    )

    if not os.path.isdir(merge_folder):
        st.error(f"❌ The path `{merge_folder}` is not a valid directory.")
        return
    os.makedirs(merge_folder, exist_ok=True)

    # 1️⃣  Base Excel upload
    base_file = st.file_uploader(
        "📂 Upload *Base* Excel File (.xlsx) — must contain a 'PAN' column",
        type=["xlsx"],
    )
    if base_file is None:
        st.info("Upload your base file first ⤴️")
        return

    try:
        base_df = pd.read_excel(base_file)
    except Exception as exc:
        st.error(f"❌  Could not read the Excel file: {exc}")
        return

    if "PAN" not in base_df.columns:
        st.error("❌  The base file has no 'PAN' column.")
        return

    base_df["PAN"] = _normalise_pan(base_df["PAN"])
    merged_df = base_df.copy()

    # 2️⃣  Discover CSVs
    st.markdown(f"🔍 Looking for CSVs in `{merge_folder}` …")
    csv_files: List[str] = [f for f in os.listdir(merge_folder) if f.lower().endswith(".csv")]
    if not csv_files:
        st.warning("⚠️  No CSV files found. Drop files into the folder and re‑run.")
        return

    st.success(f"📑 Found {len(csv_files)} file(s): {', '.join(csv_files)}")

    # 3️⃣  Merge loop — iterate through every CSV
    for fname in csv_files:
        path = os.path.join(merge_folder, fname)
        try:
            df = pd.read_csv(path)

            # Check PAN column existence
            pan_col = None
            for possible in ("PAN", "PAN Number"):
                if possible in df.columns:
                    pan_col = possible
                    break
            if not pan_col:
                st.warning(f"⚠️ Skipping {fname} — no 'PAN' or 'PAN Number' column.")
                continue

            df = df.rename(columns={pan_col: "PAN"})

            if not isinstance(df["PAN"], pd.Series):
                raise ValueError(f"'PAN' column is not a pandas Series in file: {fname}")

            # Safe PAN cleaning
            df["PAN"] = df["PAN"].apply(lambda x: str(x).strip().upper() if pd.notna(x) else "")

            # Drop contact-level junk
            cols_to_drop = UNWANTED_COLS & set(df.columns)
            if cols_to_drop:
                df = df.drop(columns=list(cols_to_drop))

            df = df.drop_duplicates(subset="PAN", keep="first")

            new_cols = [c for c in df.columns if c != "PAN" and c not in merged_df.columns]
            merged_df = merged_df.merge(df[["PAN"] + new_cols], on="PAN", how="left")

            st.success(f"✅ Merged {fname}")
        except Exception as e:
            st.error(f"❌ Error processing {fname}: {e}")

    # 4️⃣  Optional: Re‑order columns to match the canonical schema first
    ordered_cols = [c for c in EXPECTED_COLS if c in merged_df.columns]
    remaining_cols = [c for c in merged_df.columns if c not in ordered_cols]
    merged_df = merged_df[ordered_cols + remaining_cols]

    # 5️⃣  Download section
    st.markdown("---")
    st.write("**📌 Final columns (top‑of‑mind schema reminder):**")
    st.write(merged_df.columns.tolist())

    output = BytesIO()
    merged_df.to_excel(output, index=False, engine="openpyxl")
    output.seek(0)

    st.download_button(
        "⬇️  Download Final Merged File",
        data=output,
        file_name="merged_output.xlsx",
        mime="application/vnd.openxmlformats‑officedocument.spreadsheetml.sheet",
    )


# Run directly with `streamlit run merge_csv_streamlit.py`
if __name__ == "__main__":
    render_page()
