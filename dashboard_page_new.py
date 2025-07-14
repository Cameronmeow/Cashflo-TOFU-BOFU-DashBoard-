# ─────────────────────────────────────────────
# dashboard_page.py
# ─────────────────────────────────────────────
"""
Streamlit page that runs the TOFU/BOFU pipeline and lets users
download the base and processed Excel files.

Usage (inside main app.py)
--------------------------
    import dashboard_page as dash

    if nav_choice == "📊 Vendor Dashboard":
        dash.render(
            pull_module=pull,
            calc_module=calc,
            logo_path="logo.webp"
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
    """Render the vendor analytics dashboard."""

    # ───────── Header ─────────
    st.image(logo_path, width=120)
    st.title("📊 Cashflo Vendor Category and Summary")
    st.markdown(
        """
        Welcome to the **TOFU / BOFU Analysis Dashboard**.  
        This tool pulls vendor invoice data, performs monthly and quarterly calculations,
        and gives you actionable insights like:

        - Buyer Revenue Share  
        - Days Advanced, APR, Discounting Metrics  
        - TOFU/BOFU categorisation over 6, 12, 18 months  
        - Automatically grouped by PAN, Vendor, and Buyer  
        

        ---
        """
    )

    # st.divider()

    # ───────── User Config ─────────
    st.subheader("⚙️ Configuration Options")

    months = st.slider(
        "How many months of history should we analyze?",
        min_value=3, max_value=24, value=6, step=1,
        help="This controls the look-back window for TOFU/BOFU categorization logic."
    )


    st.caption("You can modify these settings anytime before running the pipeline.")

    run_btn = st.button("🚀 Run TOFU/BOFU Analysis")

    # ───────── Pipeline Run ─────────
    if run_btn:
        st.subheader("🔍 Step 1: Fetching Raw Invoice Data")
        with st.spinner("Connecting to database and pulling raw records…"):
            base_path = pull_module.run(months)

        st.success("✅ Data pull complete!")
        st.markdown(f"**Raw file saved at:** `{base_path}`")

        with open(base_path, "rb") as f:
            st.download_button(
                "⬇️ Download Raw Excel File",
                f,
                file_name=os.path.basename(base_path),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Optional: Add back calculation block if needed
        # st.subheader("📈 Step 2: Running Summary Calculations")
        # final_path = base_path.replace(".xlsx", "_processed.xlsx")
        # shutil.copy(base_path, final_path)

        # status_box = st.empty()
        # progress   = st.progress(0)
        # log_steps: list[str] = []

        # def _logger(msg: str) -> None:
        #     status_box.markdown(f"🔄 {msg}")
        #     log_steps.append(msg)
        #     progress.progress(min(len(log_steps) / expected_steps, 1.0))

        # with st.spinner("Processing metrics and generating dashboards…"):
        #     calc_module.run(final_path, progress_callback=_logger)

        # progress.progress(1.0)
        # status_box.markdown("✅ Summary generation complete.")
        # st.success("✅ Final report ready.")

        # with open(final_path, "rb") as f:
        #     st.download_button(
        #         "⬇️ Download Final Processed File",
        #         f,
        #         file_name=os.path.basename(final_path),
        #         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        #     )

        st.balloons()

    # ───────── Footer ─────────