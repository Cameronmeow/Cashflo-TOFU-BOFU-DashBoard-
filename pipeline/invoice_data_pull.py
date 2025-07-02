# pipeline/invoice_data_pull.py
from __future__ import annotations
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
from textwrap import dedent

# ──────────────────────────────────────────────────────────
# CORE – call this from Streamlit or a notebook
# ──────────────────────────────────────────────────────────
# utils/sql.py  (or inside the same module)
def _quote(dt: str) -> str:
    """
    >>> quote("i.createdAt")             ->  i."createdAt"
    >>> quote("epri.toBeClearedOnUtc")   ->  epri."toBeClearedOnUtc"
    """
    tbl, col = dt.split(".", 1)
    return f'{tbl}."{col}"'


def run_invoice_pull(
    from_date,
    to_date,
    granularity: str = "daily",
    date_type: str = "i.createdAt",
    out_dir: str = "Output",
) -> pd.DataFrame:
    """
    Pull invoice-level data (incl. Buyer Revenue Share logic) for **one month**.

    Returns the DataFrame and writes an Excel file in <out_dir>.
    """
    # 1 ── DB creds (pick up from env or hard-code while testing)
    PG_USER     = os.getenv("PG_USER",     "debashish_das")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "kmvwirnwrfw3419fd")
    PG_HOST     = os.getenv("PG_HOST",     "pg-main-replica.aps1.prod.cashflo.dev")
    PG_DB       = os.getenv("PG_DB",       "cashflo")

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DB}",
        connect_args={"options": "-c statement_timeout=0"},
    )

    # 2 ── Month boundaries
    # 2️⃣  choose bucket expression
    quoted_dt = _quote(date_type)  # ensures proper quoting like i."createdAt"

    if granularity == "weekly":
        bucket_sql = f'DATE_TRUNC(\'week\', {quoted_dt}) AS "Week Start"'
    else:
        bucket_sql = f'DATE_TRUNC(\'day\', {quoted_dt}) AS "Day"'
    
    
    # 3 ── Invoice-level query (no aggregation)
    query = dedent(f"""
        SELECT
            {bucket_sql},
            {quoted_dt}                         AS "Invoice Timestamp",
            vendororg."PAN"                     AS "PAN",
            vendororg."legalName"               AS "Supplier Name",
            buyerorg."legalName"                AS "Buyer Name",
            buyerorg."id"                       AS "Buyer Org ID",
            i."invoiceNumber",
            i."amount"                          AS "Invoice Amount",
            i."createdAt",
            i."updatedAt",
            i."masterStatusId",
            epri."effectiveDiscount",
            epri."effectiveDiscountRate",
            epri."daysAdvanced",
            epri."apr",
            epr."platformFee"

        FROM discounting."Invoice"                    i
        LEFT JOIN discounting."EarlyPaymentRequestInvoice" epri
               ON i.id = epri."invoiceId"
        LEFT JOIN discounting."EarlyPaymentRequest"        epr
               ON epr.id = epri."eprId"
        LEFT JOIN tenant."Partner"                         p
               ON p.id = COALESCE(i."partnerId", epr."partnerId")
        LEFT JOIN tenant."Organization"           vendororg  ON vendororg.id = p."vendorOrgId"
        LEFT JOIN tenant."Organization"           buyerorg   ON buyerorg.id = p."buyerOrgId"
        WHERE {quoted_dt} BETWEEN '{from_date}' AND '{to_date}'
        
    """)

    df = pd.read_sql_query(query, engine)

    # 4 ── Buyer Revenue Share (same rules as your huge SQL CASE)
    def buyer_share(r: pd.Series) -> float:
        bid  = int(r["Buyer Org ID"]) if pd.notna(r["Buyer Org ID"]) else 0
        ed   = r["effectiveDiscount"] or 0
        amt  = r["Invoice Amount"]    or 0
        apr  = r["apr"]               or 0
        days = r["daysAdvanced"]      or 0
        rate = r["effectiveDiscountRate"] or 0

        # --- flat % table ---
        flats = {
            0.0875: {448, 9916, 158109},
            0.095:  {586},
            0.10:   {10963, 11326, 11, 246800, 275674},
            0.15:   {24217, 136067, 4752, 154673},
            0.14:   {22483, 199095},
            0.13:   {368},
            0.18:   {193694},
            0.20:   {8933},
        }
        for pct, ids in flats.items():
            if bid in ids:
                return ed * pct
        if bid == 688:
            return ed * (0.12 if apr < 15 else 0.15)

        # % of ED-rate × amount
        if bid == 379:
            return (rate / 100 * amt) * 0.35

        # spread-based
        spreads = {
            (66,452,546,431): (7.0, 0.20),
            (11323,):        (6.5, 0.16),
            (8672,):         (8.0, 0.10),
            (1437,):         (6.5, 0.20),
            (153,):          (9.0, 0.35),
            (55,):           (7.34,0.11),
            (196860,196029): (8.0, 0.50),
            (38,):           (10.0,0.15),
        }
        for ids,(base,share) in spreads.items():
            if bid in ids:
                return ((apr-base)*amt*days/36500)*share

        # Zydus tier
        if bid in {24814, 11111, 128999}:
            net_apr = apr - 8
            if net_apr <= 0: return 0
            if   amt < 15e7: pct = 0.125
            elif amt < 25e7: pct = 0.15
            else:            pct = 0.175
            return pct * net_apr * amt / 100

        return 0.0

    df["Buyer Revenue Share"] = df.apply(buyer_share, axis=1)

    # 5 ── tidy up and save
    bucket_name = "Day" if granularity == "daily" else "Week Start"
    df.sort_values([bucket_name, "PAN", "invoiceNumber"], inplace=True)

    # 🔧 Fix timezone issue
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_convert(None)   # <-- MUST BE BEFORE to_excel

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(
        out_dir, f"invoice_metrics_{from_date}_{to_date}_{granularity}.xlsx"
    )
    df.to_excel(out_path, index=False)   # <- now this will not crash
    return df  # caller can still use the DataFrame


# ──────────────────────────────────────────────────────────
# Simple CLI utility  (optional)
# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse, pathlib

    parser = argparse.ArgumentParser(description="Invoice-level data pull")
    parser.add_argument("month", help="Month in YYYY-MM format (e.g. 2024-03)")
    parser.add_argument("--granularity", choices=["daily", "weekly"], default="daily")
    parser.add_argument("--date-type", default="i.createdAt")
    parser.add_argument("--out-dir", default="Output")
    args = parser.parse_args()

    df_out = run_invoice_pull(
        month=args.month,
        granularity=args.granularity,
        date_type=args.date_type,
        out_dir=args.out_dir,
    )
    print(f"✅  Pulled {len(df_out):,} rows → {pathlib.Path(args.out_dir).resolve()}")
