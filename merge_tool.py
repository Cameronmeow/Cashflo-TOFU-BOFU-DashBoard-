import pandas as pd
import streamlit as st
from io import BytesIO
import os

def render_page():
    st.subheader("📁 Auto-Merge All CSVs into Base Excel")

    merge_folder = "./Hubspot files"
    os.makedirs(merge_folder, exist_ok=True)

    # 1️⃣ Upload the base file
    base_file = st.file_uploader(
        "📂 Upload Base Excel File (.xlsx) — must contain a 'PAN' column", type=["xlsx"]
    )
    if not base_file:
        st.info("Upload your base file first ⤴️")
        return

    base_df = pd.read_excel(base_file)
    if "PAN" not in base_df.columns:
        st.error("❌  The base file has no 'PAN' column.")
        return

    # Keep PAN trimmed / uppercase for reliable joins
    base_df["PAN"] = base_df["PAN"].astype(str).str.strip().str.upper()
    merged_df = base_df.copy()

    # 2️⃣ Find CSVs
    st.markdown(f"🔍 Looking for CSVs in `{merge_folder}` …")
    csv_files = [f for f in os.listdir(merge_folder) if f.lower().endswith(".csv")]
    if not csv_files:
        st.warning("⚠️ No CSV files found. Drop files into the folder.")
        return
    st.success(f"📑 Found {len(csv_files)} file(s): {', '.join(csv_files)}")

    # Contact / metadata columns to drop if present
    UNWANTED_COLS = {
        "First Name",
        "Last Name",
        "Last Contacted",
        "Number of times contacted",
        "Designation of Contact Person",
        "Title",
        "Job Title",
        "KDM",
        "Contact ID",
    }

    # 3️⃣ Merge each CSV
    for fname in csv_files:
        path = os.path.join(merge_folder, fname)
        try:
            df = pd.read_csv(path)

            # Require identifier
            if "PAN Number" not in df.columns:
                st.warning(f"⚠️ Skipping {fname} — no 'PAN Number' column.")
                continue

            # Standardise PAN
            df = df.rename(columns={"PAN Number": "PAN"})
            df["PAN"] = df["PAN"].astype(str).str.strip().str.upper()

            # Drop any of the unwanted columns that are actually present
            cols_to_drop = UNWANTED_COLS & set(df.columns)
            if cols_to_drop:
                df = df.drop(columns=list(cols_to_drop))

            # Deduplicate on PAN to prevent one-to-many merges
            df = df.drop_duplicates(subset="PAN", keep="first")

            # Select only truly new columns (plus PAN)
            new_cols = ["PAN"] + [c for c in df.columns if c != "PAN" and c not in merged_df.columns]
            merged_df = merged_df.merge(df[new_cols], on="PAN", how="left")

            st.success(f"✅ Merged {fname}")
        except Exception as e:
            st.error(f"❌ Error processing {fname}: {e}")

    # 4️⃣ Download
    st.markdown("---")
    st.write("📌 Final columns:")
    st.write(list(merged_df.columns))

    output = BytesIO()
    merged_df.to_excel(output, index=False, engine="openpyxl")
    output.seek(0)

    st.download_button(
        "⬇️ Download Final Merged File",
        data=output,
        file_name="merged_output.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
