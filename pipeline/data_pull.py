# pipeline/data_pull.py
import os, pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from textwrap import dedent
from dotenv import load_dotenv  
load_dotenv()  
def run(months_back: int = 6, out_dir="Output",date_type: str = 'i."createdAt"' ) -> str:
    """Return the path to the freshly-written Excel file."""
    # DB creds from env ­­­(set them once in your shell or .env)
    # PG_USER = os.getenv("PG_USER", "debashish_das")
    PG_USER = os.getenv("PG_USER")
    # PG_PASSWORD = os.getenv("PG_PASSWORD", "kmvwirnwrfw3419fd")
    PG_PASSWORD = os.getenv("PG_PASSWORD")
    # PG_HOST = os.getenv("PG_HOST", "pg-main-replica.aps1.prod.cashflo.dev")
    PG_HOST = os.getenv("PG_HOST")
    # PG_DB = os.getenv("PG_DB", "cashflo")
    PG_DB = os.getenv("PG_DB")


    cutoff = (datetime.now() - timedelta(days=30*months_back)).date().isoformat()
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DB}",
        connect_args={"options": "-c statement_timeout=0"}
    )

    QUERY = dedent(f"""SELECT
    "Month",
    vendororg."PAN"                        AS "PAN",
    vendororg."legalName"                  AS "Supplier Name",
    buyerorg."legalName"                   AS "Buyer Name",
    buyerorg."id"                          AS "Buyer Org ID",
    p."isEligible"                         AS "Eligibility",
    ROUND("TOFU" / 100000, 2)              AS "TOFU (in lacs)",
    ROUND("BOFU" / 100000, 2)              AS "BOFU (in lacs)",
    ROUND("Credit Period",      2)         AS "Credit Period",
    ROUND("Max Days Advanced",  2)         AS "Max Days Advanced",
    ROUND("Days Advanced",      2)         AS "Days Advanced",
    ROUND("Effective Discount" / 100000, 2)AS "Effective Discount (in lacs)",
    ROUND("Platform Fee"       / 100000, 2)AS "Platform Fee (in lacs)",
    ROUND("Acc Rate",           2)         AS "Acc Rate",
    vendororg."relationshipManagerName"    AS "RM Name",
    ROUND("APR",                2)         AS "APR"
FROM (
    /* ── inner aggregation unchanged except for Platform Fee ─────────────── */
    SELECT
        DATE_TRUNC('Month', COALESCE(i."createdAt", epri."activatedOn")) AS "Month",
        p.id                                     AS "Partner ID",
        SUM(i."amount")                          AS "TOFU",
        MAX(epri."requestPlacedAmount")          AS "BOFU",
        SUM(
            CAST(
                DATE_PART(
                    'day',
                    DATE_TRUNC('day', i."dueDateAtUtc")
                    - DATE_TRUNC('day', i."generatedAtUtc")
                )
            AS NUMERIC) * i."amount"
        ) / NULLIF(SUM(i."amount"), 0)           AS "Credit Period",
        SUM(
            CAST(
                DATE_PART(
                    'day',
                    DATE_TRUNC('day', i."dueDateAtUtc" AT TIME ZONE 'UTC')
                    - DATE_TRUNC('day', i."createdAt"  AT TIME ZONE 'UTC')
                ) - p."settlementDays"
            AS NUMERIC) * i."amount"
        ) / NULLIF(SUM(i."amount"), 0)           AS "Max Days Advanced",
        MAX(epri."weightedDaysAdvanced")         AS "Days Advanced",
        MAX(epri."totalEffectiveDiscount")       AS "Effective Discount",
        MAX(epri."totalPlatformFee")             AS "Platform Fee",
        (COALESCE(MAX(epri."requestPlacedAmount"), 0)
         / NULLIF(SUM(i."amount"), 0)) * 100     AS "Acc Rate",
        ((MAX(epri."totalEffectiveDiscount")
          / NULLIF(MAX(epri."requestPlacedAmount"), 0))
         * (365 / NULLIF(MAX(epri."weightedDaysAdvanced"), 0))) * 100 AS "APR"
    FROM (
        /* — DISTINCT ON most recent invoice per invoiceNumber / FY bucket — */
        SELECT DISTINCT ON (
            trim(i1."invoiceNumber"),
            i1."partnerId",
            CASE
              WHEN EXTRACT(month FROM i1."generatedAtUtc") >= 4
              THEN EXTRACT(year  FROM i1."generatedAtUtc")
              ELSE EXTRACT(year  FROM i1."generatedAtUtc") - 1
            END
        ) i1.*
        FROM discounting."Invoice" i1
        WHERE i1."amount" > 0
          AND DATE_TRUNC('day', i1."dueDateAtUtc") >
              DATE_TRUNC('day', i1."createdAt") + INTERVAL '1 day'
          AND DATE_TRUNC('day', i1."createdAt") <
              DATE_TRUNC('day', i1."generatedAtUtc") + INTERVAL '180 day'
        ORDER BY trim(i1."invoiceNumber"),
                 i1."partnerId",
                 CASE
                   WHEN EXTRACT(month FROM i1."generatedAtUtc") >= 4
                   THEN EXTRACT(year  FROM i1."generatedAtUtc")
                   ELSE EXTRACT(year  FROM i1."generatedAtUtc") - 1
                 END,
                 i1."createdAt" ASC
    ) i
    FULL JOIN (
        /* — Aggregated Early-Payment metrics plus Platform Fee — */
        SELECT
            epr."partnerId",
            DATE_TRUNC('month', epr."activatedOn")         AS "activatedOn",
            SUM(i."amount")                                AS "requestPlacedAmount",
            SUM(COALESCE(epri."daysAdvanced", 0) * i."amount")
              / SUM(i."amount")                           AS "weightedDaysAdvanced",
            SUM(epri."effectiveDiscount")                  AS "totalEffectiveDiscount",
            SUM(epr."platformFee")                         AS "totalPlatformFee"
        FROM discounting."EarlyPaymentRequest" epr
        INNER JOIN discounting."EarlyPaymentRequestInvoice" epri
               ON epr.id = epri."eprId"
        INNER JOIN discounting."Invoice" i
               ON i.id  = epri."invoiceId"
        WHERE epri."eprInvoiceStatusId" IN (0,1,2)
        GROUP BY epr."partnerId",
                 DATE_TRUNC('month', epr."activatedOn")
    ) epri
      ON i."partnerId" = epri."partnerId"
     AND DATE_TRUNC('month', COALESCE(i."createdAt", epri."activatedOn"))
         = epri."activatedOn"
    LEFT JOIN tenant."Partner" p
           ON p.id = COALESCE(i."partnerId", epri."partnerId")
    WHERE DATE_TRUNC('month', COALESCE(i."createdAt", epri."activatedOn"))
          >= '{cutoff}'
    GROUP BY "Month", p.id
) subquery
LEFT JOIN tenant."Partner"       p         ON subquery."Partner ID" = p."id"
LEFT JOIN tenant."Organization"  vendororg ON vendororg.id          = p."vendorOrgId"
LEFT JOIN tenant."Organization"  buyerorg  ON buyerorg.id           = p."buyerOrgId"
ORDER BY "Month" DESC;""")
    print("Running monthly metrics query …")
    df = pd.read_sql_query(QUERY, engine)

    # buyer-revenue calc (exactly what you already wrote)
    # strip tz from Month
    if df["Month"].dtype.kind == "M":
        df["Month"] = df["Month"].dt.tz_localize(None)
    df['Effective Discount Rate'] = df['Effective Discount (in lacs)'] / df['TOFU (in lacs)'] * 100
    df['Platform Fee Rate'] = df['Platform Fee (in lacs)']/df['BOFU (in lacs)'] * 100

    def compute_buyer_revenue(row):
        bid = int(row["Buyer Org ID"])
        ed  = row["Effective Discount (in lacs)"] * 100000  # convert to rupees
        amt = row["Platform Fee (in lacs)"] * 100000  # convert to rupees
        apr = row["APR"]
        days = row["Days Advanced"]
        rate = row["Effective Discount Rate"]

        # 1) Flat % of Effective Discount
        flats = {
            0.0875: {448, 9916, 158109},
            0.095:  {586},
            0.10:   {10963,11326,11,246800,275674},
            0.15:   {24217,136067,4752,154673},
            0.14:   {22483,199095},
            0.13:   {368},
            0.18:   {193694},
            0.20:   {8933},
        }
        for pct, ids in flats.items():
            if bid in ids:
                return ed * pct

        if bid == 688:
            return ed * (0.12 if apr < 15 else 0.15)

        # 2) % of Effective Discount Rate x Invoice Amount
        if bid == 379:
            return (rate/100 * amt) * 0.35

        # 3) Interest Spread Based
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
        for ids, (base, share) in spreads.items():
            if bid in ids:
                return ((apr - base) * amt * days / 36500) * share

        # 4) Zydus tiered logic
        if bid in {24814,11111,128999}:
            # use BOFU lacs → convert back to rupees
            bofu = row["BOFU (in lacs)"] * 100000
            net_apr = apr - 8.0
            if bofu < 15e7:      pct = 0.125
            elif bofu < 25e7:    pct = 0.15
            else:                pct = 0.175
            return pct * net_apr * amt

        # Default
        return 0.0

    df["Buyer Revenue Share"] = df.apply(compute_buyer_revenue, axis=1)
    df['Wtd Credit Period- Calculated'] = df['TOFU (in lacs)'] * df['Credit Period']
    df['Wtd Max Days-Calculated'] = df['TOFU (in lacs)'] * df['Max Days Advanced']
    df['Wtd Act Days-Calculated'] = df['TOFU (in lacs)'] * df['Days Advanced']
    df['Wtd APR'] = df['TOFU (in lacs)'] * df['APR']
    # Remove timezone info for Excel compatibility
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_convert(None)


    #  … compute Buyer Revenue Share & weighted fields …

    out_file = os.path.join(out_dir, f"cashflo_metrics_{months_back}m.xlsx")
    os.makedirs(out_dir, exist_ok=True)
    df.to_excel(out_file, index=False)
    return out_file
