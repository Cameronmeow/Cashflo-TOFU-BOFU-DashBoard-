# ─────────────────────────────────────────────
# dashboard_page.py
# ─────────────────────────────────────────────
"""
Streamlit page that runs the TOFU/BOFU pipeline and lets users
download the base and processed Excel files.

Usage (inside main app.py)
--------------------------
    import dashboard_page as dash

    if nav_choice == "📊 Dashboard":
        dash.render(
            pull_module=pull,         # your data-pull module
            calc_module=calc,         # your calc_all module
            logo_path="logo.webp"     # path to logo image
        )
"""

from __future__ import annotations
import os
import shutil
from typing import Callable

import streamlit as st


def render(
    pull_module,
    calc_module,
    logo_path: str = "logo.webp",
    expected_steps: int = 3,
) -> None:
    """Render the dashboard page.

    Parameters
    ----------
    pull_module   : module with .run(months:int) -> str
    calc_module   : module with .run(path:str, progress_callback=Callable)
    logo_path     : path to the company logo
    expected_steps: how many progress-log messages you expect from calc.run()
    """
    # ───────── Header ─────────
    st.image(logo_path, width=120)
    st.title("📊 CashFlo Vendor Metrics Dashboard")
    st.markdown(
        """
        Welcome to the **TOFU / BOFU Analysis Dashboard**.  
        This tool pulls vendor invoice data, performs monthly and quarterly calculations,
        and gives you actionable insights like:

        - Buyer Revenue Share  
        - Vendor Category (TOFU/BOFU)
        - Days Advanced, APR, Discounting Metrics
        - TOFU/BOFU categorisation over 6, 12 months
        - Automatically grouped by PAN, Vendor, and Buyer
        ---
        """
    )

    # ───────── User Inputs ─────────
    st.subheader("🔧 Configuration")
    months = st.slider(
        "Select look-back window for TOFU/BOFU categorisation:",
        min_value=3, max_value=24, value=6, step=1,
    )
    

    run_btn = st.button("🚀 Run Full Pipeline")

    # ───────── Main Pipeline ─────────
    if run_btn:
        # STEP 1 – Data Pull
        st.subheader("⚙️ Step 1: Fetching Data from Database")
        with st.spinner("Connecting to DB & downloading data…"):
            base_path = pull_module.run(months)  # <= your pull.run signature
        st.success("✅ Base Excel file created!")
        st.markdown(f"**Base file saved to:** `{base_path}`")

        with open(base_path, "rb") as f:
            st.download_button(
        "⬇️ Download *Base* Excel File",
        f,
        file_name=os.path.basename(base_path),
      mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

        # STEP 2 – Calculations
        # st.subheader("📈 Step 2: Running Calculations & Building Summary Sheets")
        # final_path = base_path.replace(".xlsx", "_processed.xlsx")
        # shutil.copy(base_path, final_path)

        # status_box = st.empty()
        # progress   = st.progress(0)
        # log_steps: list[str] = []

        # def _logger(msg: str) -> None:
        #     status_box.markdown(f"🔄 {msg}")
        #     log_steps.append(msg)
        #     # simple equal-weight progress
        #     progress.progress(min(len(log_steps) / expected_steps, 1.0))

        # with st.spinner("Crunching numbers…"):
        #     calc_module.run(final_path, progress_callback=_logger)

        # progress.progress(1.0)
        # status_box.markdown("✅ All sheets generated!")
        # st.success("✅ Calculations finished & saved.")

        # # STEP 3 – Download processed file
        # st.subheader("📥 Download Final Output")
        # with open(final_path, "rb") as f:
        #     st.download_button(
        #         "⬇️ Download *Processed* Excel File",
        #         f,
        #         file_name=os.path.basename(final_path),
        #         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        #     )

        st.balloons()

   