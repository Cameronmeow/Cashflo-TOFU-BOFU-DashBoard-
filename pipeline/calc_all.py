# pipeline/calc_all.py
from pipeline.calculations_1 import build_month_pivot
from pipeline.calculations_2 import build_quarter_metrics
from pipeline.calculations_3 import build_supplier_pivot

def run(excel_path, progress_callback=None):
    logs = []

    def log(msg):
        logs.append(msg)
        if progress_callback:
            progress_callback(msg)

    log("🧩 Creating monthly pivot sheet…")
    build_month_pivot(excel_path)

    log("📊 Building quarterly metrics sheet…")
    build_quarter_metrics(excel_path)

    log("📉 Creating supplier-level raw pivot…")
    build_supplier_pivot(excel_path)

    log("✅ All sheets generated!")

    return logs  # ✅ Fix: return the log list