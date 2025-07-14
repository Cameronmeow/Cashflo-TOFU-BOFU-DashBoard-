import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine
from textwrap import dedent
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

def run(
    months_back: int   = 6,
    out_dir:    str    = "Output",
    **kwargs,
) -> str:
    """Return the path to the freshly-written Excel file."""
    # 1) Load database credentials from environment
    PG_USER = os.getenv("PG_USER")
    PG_PASSWORD = os.getenv("PG_PASSWORD")
    PG_HOST = os.getenv("PG_HOST")
    PG_DB = os.getenv("PG_DB")

    if not all([PG_USER, PG_PASSWORD, PG_HOST, PG_DB]):
        raise EnvironmentError("One or more PostgreSQL credentials are missing in the environment.")

    # 2) Compute cutoff date based on months_back
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=30 * months_back)
    cutoff_str = cutoff_dt.date().isoformat()

    # 3) Create database engine
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DB}",
        connect_args={"options": "-c statement_timeout=0"}
    )

    # 4) Define the SQL query with dynamic cutoff filter
    QUERY = dedent(f"""
    WITH
-- 1) Invoice‐ and EPR‐level revenue summary
RequestSummary AS (
  SELECT
    epr."partnerId"                               AS "Partner ID",
    DATE_TRUNC('month', epr."activatedOn")        AS "Month",
	SUM(DISTINCT epr."platformFee")               AS "Platform Fee",
	SUM(
      GREATEST(
        CASE
          WHEN buyerorg."id" IN (128999,11111,24814,163022) THEN
            ((epri."apr"-8)*inv."amount"*epri."daysAdvanced")/36500
            * CASE
                WHEN inv."amount" < 150000000 THEN 0.125
                WHEN inv."amount" BETWEEN 150000000 AND 250000000 THEN 0.15
                ELSE 0.175
              END
          WHEN buyerorg."id" IN (448,9916,158109) THEN epri."effectiveDiscount"*0.0875
          WHEN buyerorg."id" = 586            THEN epri."effectiveDiscount"*0.095
          WHEN buyerorg."id" IN (10963,11326,11,246800,275674)
                                               THEN epri."effectiveDiscount"*0.10
          WHEN buyerorg."id" IN (24217,136067,4752,154673)
                                               THEN epri."effectiveDiscount"*0.15
          WHEN buyerorg."id" = 379
                                               THEN (epri."effectiveDiscountRate"*inv."amount"/100)*0.35
          WHEN buyerorg."id" IN (22483,199095)
                                               THEN epri."effectiveDiscount"*0.14
          WHEN buyerorg."id" = 368            THEN epri."effectiveDiscount"*0.13
          WHEN buyerorg."id" = 193694         THEN epri."effectiveDiscount"*0.18
          WHEN buyerorg."id" IN (66,452,546,431)
            THEN (((epri."apr"-7)*inv."amount"*epri."daysAdvanced")/36500)*0.20
          WHEN buyerorg."id" = 11323
            THEN (((epri."apr"-6.5)*inv."amount"*epri."daysAdvanced")/36500)*0.16
          WHEN buyerorg."id" = 8672
            THEN (((epri."apr"-8)*inv."amount"*epri."daysAdvanced")/36500)*0.10
          WHEN buyerorg."id" = 1437
            THEN (((epri."apr"-6.5)*inv."amount"*epri."daysAdvanced")/36500)*0.20
          WHEN buyerorg."id" = 153
            THEN (((epri."apr"-9)*inv."amount"*epri."daysAdvanced")/36500)*0.35
          WHEN buyerorg."id" = 55
            THEN (((epri."apr"-7.34)*inv."amount"*epri."daysAdvanced")/36500)*0.11
          WHEN buyerorg."id" = 8933
            THEN (((epri."apr"-8.5)*inv."amount"*epri."daysAdvanced")/36500)*0.20
          WHEN buyerorg."id" IN (196860,196029)
            THEN (((epri."apr"-8)*inv."amount"*epri."daysAdvanced")/36500)*0.50
          WHEN buyerorg."id" = 38
            THEN (((epri."apr"-10)*inv."amount"*epri."daysAdvanced")/36500)*0.15
          WHEN buyerorg."id" = 2795
            THEN epri."effectiveDiscount"
                 * (
                     EXTRACT(DAY FROM inv."estimatedDueDateAtUtc" - inv."dueDateAtUtc")::float
                     / EXTRACT(DAY FROM inv."estimatedDueDateAtUtc" - epri."toBeClearedOnUtc")::float
                   ) * 0.25
          WHEN buyerorg."id" = 11625 THEN
            CASE
              WHEN (epri."apr" - epri."apr"*0.14) > 10
                THEN epri."effectiveDiscount"*0.14
              ELSE ((epri."apr"-10)*inv."amount"*epri."daysAdvanced")/36500
            END
          WHEN buyerorg."id" = 24505 THEN
            CASE
              WHEN epri."apr"/1.15 < 10.25
                THEN epri."effectiveDiscount"*(epri."apr"-10.25)/100
              ELSE epri."effectiveDiscount"*0.15
            END
          WHEN buyerorg."id" = 688 THEN
            CASE
              WHEN epri."apr" < 15
                THEN epri."effectiveDiscount"*0.12
              ELSE epri."effectiveDiscount"*0.15
            END
          ELSE 0
        END,
      0)
    )                                            AS "Buyer Revenue Share"
  FROM discounting."EarlyPaymentRequest"        epr
  JOIN discounting."EarlyPaymentRequestInvoice" epri
    ON epr."id" = epri."eprId"
  JOIN discounting."Invoice"                   inv
    ON inv."id" = epri."invoiceId"
  JOIN tenant."Partner"                        p2
    ON p2."id" = epr."partnerId"
  JOIN tenant."Organization"                   buyerorg
    ON buyerorg."id" = p2."buyerOrgId"
  WHERE epri."eprInvoiceStatusId" IN (0,1,2)
  GROUP BY 1,2
),

-- 2) Monthly vendor×buyer summary
-- 2) Monthly vendor×buyer summary (from start to today)
VendorData AS (
SELECT
  DATE_TRUNC('Month', COALESCE(i."createdAt", epri."activatedOn")) AS "Month",
        p.id AS "Partner ID",
        SUM(i."amount") AS "TOFU",
        MAX(epri."requestPlacedAmount") AS "BOFU",
        SUM(CAST(DATE_PART('day', DATE_TRUNC('day', i."dueDateAtUtc"::timestamp) - DATE_TRUNC('day', i."generatedAtUtc"::timestamp)) AS numeric) * i."amount") 
            / NULLIF(SUM(i."amount"), 0) AS "Credit Period",
        SUM(CAST(DATE_PART('day', DATE_TRUNC('day', i."dueDateAtUtc"::timestamp AT TIME ZONE 'UTC') 
            - DATE_TRUNC('day', i."createdAt"::timestamp AT TIME ZONE 'UTC')) 
            - p."settlementDays" AS NUMERIC) * i."amount") 
            / NULLIF(SUM(i."amount"), 0) AS "Max Days Advanced",
        MAX(epri."weightedDaysAdvanced") as "Days Advanced",
        MAX(epri."totalEffectiveDiscount") as "Effective Discount",
        (COALESCE(MAX(epri."requestPlacedAmount"), 0)/NULLIF(SUM(i."amount"), 0))*100 as "Acc Rate",
        ((MAX(epri."totalEffectiveDiscount")/NULLIF(MAX(epri."requestPlacedAmount"), 0)) * 
    (365/NULLIF(MAX(epri."weightedDaysAdvanced"), 0)))*100 as "APR"
    FROM (
        SELECT DISTINCT ON (trim(i1."invoiceNumber"), i1."partnerId", 
                   CASE 
                        WHEN extract(month from i1."generatedAtUtc") >= 4 THEN extract(year from i1."generatedAtUtc")
                        ELSE extract(year from i1."generatedAtUtc") - 1
                    END) i1.*
        FROM discounting."Invoice" i1
        where i1."amount" > 0 and date_trunc('day', i1."dueDateAtUtc") > date_trunc('day', i1."createdAt") + interval '1 day'
        and date_trunc('day', i1."createdAt") < date_trunc('day', i1."generatedAtUtc") + interval '180 day'
        ORDER BY trim(i1."invoiceNumber"), i1."partnerId", 
                    CASE 
                        WHEN extract(month from i1."generatedAtUtc") >= 4 THEN extract(year from i1."generatedAtUtc")
                        ELSE extract(year from i1."generatedAtUtc") - 1
                    END, i1."createdAt" asc
    ) i
    FULL JOIN (
        SELECT
            epr."partnerId",
            DATE_TRUNC('month', epr."activatedOn") as "activatedOn",
            SUM(i."amount") as "requestPlacedAmount",
            SUM(COALESCE(epri."daysAdvanced", 0) * i."amount")/SUM(i."amount") as "weightedDaysAdvanced",
            SUM(epri."effectiveDiscount") as "totalEffectiveDiscount"
        FROM discounting."EarlyPaymentRequest" epr
        INNER JOIN discounting."EarlyPaymentRequestInvoice" epri
            ON epr.id = epri."eprId"
        INNER JOIN discounting."Invoice" i
            ON i.id = epri."invoiceId"
        WHERE epri."eprInvoiceStatusId" in (0,1,2)
        GROUP BY epr."partnerId", DATE_TRUNC('month', epr."activatedOn")
    ) epri
    ON i."partnerId" = epri."partnerId"
    AND DATE_TRUNC('month', COALESCE(i."createdAt", epri."activatedOn")) = epri."activatedOn"
    LEFT JOIN tenant."Partner" p
        ON p.id = coalesce(i."partnerId", epri."partnerId")
    where DATE_TRUNC('month', COALESCE(i."createdAt", epri."activatedOn")) >= DATE_TRUNC('month', '{cutoff_str}'::date)
    GROUP BY "Month", p.id
),
-- 3) Roll up to vendor×month
VendorMonthly AS (
  SELECT
    vd."Month",
    p."vendorOrgId"         AS "Vendor ID",
    SUM(vd."TOFU")          AS "TOFU",
    SUM(vd."BOFU")          AS "BOFU"
  FROM VendorData vd
  JOIN tenant."Partner" p
    ON p."id" = vd."Partner ID"
  GROUP BY 1,2
),

-- 4) Rolling sums & counts (including current row)
VendorStats AS (
  SELECT
    vm.*,

    -- 6-month sums of amounts
    COALESCE(
      SUM(vm."TOFU") OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW
      ), 0
    ) AS sum_tofu_amt_6,
    COALESCE(
      SUM(vm."BOFU") OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW
      ), 0
    ) AS sum_bofu_amt_6,

    -- 12-month sums of amounts
    COALESCE(
      SUM(vm."TOFU") OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW
      ), 0
    ) AS sum_tofu_amt_12,
    COALESCE(
      SUM(vm."BOFU") OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW
      ), 0
    ) AS sum_bofu_amt_12,

    -- 6-month counts of any TOFU/BOFU months
    COALESCE(
      SUM(CASE WHEN vm."TOFU">0 THEN 1 ELSE 0 END) OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW
      ), 0
    ) AS count_tofu_6,
    COALESCE(
      SUM(CASE WHEN vm."BOFU">0 THEN 1 ELSE 0 END) OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW
      ), 0
    ) AS count_bofu_6,

    -- 12-month counts of any TOFU/BOFU months
    COALESCE(
      SUM(CASE WHEN vm."TOFU">0 THEN 1 ELSE 0 END) OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW
      ), 0
    ) AS count_tofu_12,
    COALESCE(
      SUM(CASE WHEN vm."BOFU">0 THEN 1 ELSE 0 END) OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW
      ), 0
    ) AS count_bofu_12,

    -- last BOFU-active month before the current
    COALESCE(
      MAX(CASE WHEN vm."BOFU">0 THEN vm."Month" END) OVER (
        PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), DATE '1900-01-01'
    ) AS last_bofu_before

  FROM VendorMonthly vm
),

-- 5) Vendor-level categories (all NULL-safe)
VendorCategories AS (
  SELECT
    vs."Vendor ID",
    vs."Month",

    -- first/last TOFU & BOFU
    MIN(CASE WHEN vs."TOFU">0 THEN vs."Month" END)
      OVER (PARTITION BY vs."Vendor ID") AS first_tofu_month,
    MAX(CASE WHEN vs."TOFU">0 THEN vs."Month" END)
      OVER (PARTITION BY vs."Vendor ID") AS last_tofu_month,
    MIN(CASE WHEN vs."BOFU">0 THEN vs."Month" END)
      OVER (PARTITION BY vs."Vendor ID") AS first_bofu_month,
    MAX(CASE WHEN vs."BOFU">0 THEN vs."Month" END)
      OVER (PARTITION BY vs."Vendor ID") AS last_bofu_month,

    -- TOFU buckets by count
    CASE
      WHEN vs.count_tofu_6  >= 5 THEN 'Regular'
      WHEN vs.count_tofu_6  BETWEEN 3 AND 4 THEN 'Sporadic'
      WHEN vs.count_tofu_6  BETWEEN 1 AND 2 THEN 'Low'
      ELSE 'None'
    END AS tofu_cat_6m,

    CASE
      WHEN vs.count_tofu_12 >= 9 THEN 'Regular'
      WHEN vs.count_tofu_12 BETWEEN 5 AND 8 THEN 'Sporadic'
      WHEN vs.count_tofu_12 BETWEEN 1 AND 4 THEN 'Low'
      ELSE 'None'
    END AS tofu_cat_12m,

    -- 6-month BOFU churn/at-risk + amount-ratio
    CASE
      -- 3 most recent TOFU rows all have BOFU=0 → Churned
      WHEN vs.count_bofu_6 = 0 THEN 'Never Transacted'
      WHEN (
        SUM(CASE WHEN vs."TOFU">0 THEN 1 ELSE 0 END) OVER (
          PARTITION BY vs."Vendor ID" ORDER BY vs."Month"
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) = 3
        AND
        SUM(CASE WHEN vs."BOFU">0 THEN 1 ELSE 0 END) OVER (
          PARTITION BY vs."Vendor ID" ORDER BY vs."Month"
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) = 0
      ) THEN 'Churned'

      -- 2 most recent TOFU rows all have BOFU=0 → At Risk
      WHEN (
        SUM(CASE WHEN vs."TOFU">0 THEN 1 ELSE 0 END) OVER (
          PARTITION BY vs."Vendor ID" ORDER BY vs."Month"
          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) = 2
        AND
        SUM(CASE WHEN vs."BOFU">0 THEN 1 ELSE 0 END) OVER (
          PARTITION BY vs."Vendor ID" ORDER BY vs."Month"
          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) = 0
      ) THEN 'At Risk'

      -- high/med/low on 6-mo BOFU∶TOFU amount ratio
      WHEN vs.sum_bofu_amt_6::numeric
           / NULLIF(vs.sum_tofu_amt_6,0) >= 0.8 THEN 'High'
      WHEN vs.sum_bofu_amt_6::numeric
           / NULLIF(vs.sum_tofu_amt_6,0) >= 0.5 THEN 'Med'
      WHEN vs.sum_bofu_amt_6::numeric
           / NULLIF(vs.sum_tofu_amt_6,0) >  0   THEN 'Low'
      ELSE 'Never Transacted'
    END AS bofu_cat_6m,

    -- 12-month BOFU churn/at-risk + amount-ratio
    CASE
      WHEN vs.count_bofu_12 = 0 THEN 'Never Transacted'
      WHEN (
        SUM(CASE WHEN vs."TOFU">0 THEN 1 ELSE 0 END) OVER (
          PARTITION BY vs."Vendor ID" ORDER BY vs."Month"
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) = 3
        AND
        SUM(CASE WHEN vs."BOFU">0 THEN 1 ELSE 0 END) OVER (
          PARTITION BY vs."Vendor ID" ORDER BY vs."Month"
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) = 0
      ) THEN 'Churned'
      WHEN (
        SUM(CASE WHEN vs."TOFU">0 THEN 1 ELSE 0 END) OVER (
          PARTITION BY vs."Vendor ID" ORDER BY vs."Month"
          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) = 2
        AND
        SUM(CASE WHEN vs."BOFU">0 THEN 1 ELSE 0 END) OVER (
          PARTITION BY vs."Vendor ID" ORDER BY vs."Month"
          ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) = 0
      ) THEN 'At Risk'
      WHEN vs.sum_bofu_amt_12::numeric
           / NULLIF(vs.sum_tofu_amt_12,0) >= 0.8 THEN 'High'
      WHEN vs.sum_bofu_amt_12::numeric
           / NULLIF(vs.sum_tofu_amt_12,0) >= 0.5 THEN 'Med'
      WHEN vs.sum_bofu_amt_12::numeric
           / NULLIF(vs.sum_tofu_amt_12,0) >  0    THEN 'Low'
      ELSE 'Never Transacted'
    END AS bofu_cat_12m

  FROM VendorStats vs
)



-- 6) Final output
SELECT
  m."Month",
  vendororg."PAN"                                   AS "PAN",
  vendororg."legalName"                             AS "Supplier Name",
  buyerorg."legalName"                              AS "Buyer Name",
  p."isEligible"                                    AS "Eligibility",
  ROUND((m."TOFU"/100000)::numeric,2)               AS "TOFU (in lacs)",
  ROUND((m."BOFU"/100000)::numeric,2)               AS "BOFU (in lacs)",
  ROUND(m."Credit Period"::numeric,2)               AS "Credit Period",
  ROUND(m."Max Days Advanced"::numeric,2)           AS "Max Days Advanced",
  ROUND(m."Days Advanced"::numeric,2)               AS "Days Advanced",
  ROUND((m."Effective Discount"/100000)::numeric,2) AS "Effective Discount (in lacs)",
  ROUND(m."Acc Rate"::numeric,2)                    AS "Acc Rate",
  ROUND((rs."Platform Fee"/100000)::numeric,2)      AS "Platform Fee (in lacs)",
  ROUND((rs."Buyer Revenue Share"/100000)::numeric,2) AS "Buyer Revenue Share (in lacs)",
  vendororg."relationshipManagerName"                AS "RM Name",
  ROUND(m."APR"::numeric,2)                         AS "APR",
  vc.first_tofu_month                               AS "First TOFU Month",
  vc.last_tofu_month                                AS "Last TOFU Month",
  vc.first_bofu_month                               AS "First BOFU Month",
  vc.last_bofu_month                                AS "Last BOFU Month",
  vc.tofu_cat_6m                                    AS "TOFU Category (6m)",
  vc.tofu_cat_12m                                   AS "TOFU Category (12m)",
  vc.bofu_cat_6m                                    AS "BOFU Category (6m)",
  vc.bofu_cat_12m                                   AS "BOFU Category (12m)"
FROM VendorData m
LEFT JOIN RequestSummary rs
  ON rs."Partner ID" = m."Partner ID"
 AND rs."Month"      = m."Month"
JOIN tenant."Partner"         p         ON p."id"             = m."Partner ID"
JOIN tenant."Organization"    vendororg  ON vendororg."id"     = p."vendorOrgId"
JOIN tenant."Organization"    buyerorg   ON buyerorg."id"      = p."buyerOrgId"
LEFT JOIN VendorCategories    vc         ON vc."Vendor ID"     = p."vendorOrgId"
                                 AND vc."Month"        = m."Month"
ORDER BY m."Month" DESC
LIMIT 100000;

    """)

    # 5) Execute query
    print("Running monthly metrics query …")
    df = pd.read_sql_query(QUERY, engine)

    # 1) coerce to datetime (this will be tz‐aware because your SQL has UTC timestamps)
    df['Month'] = pd.to_datetime(df['Month'], utc=True)

    # 2) strip the timezone off so that dtype is datetime64[ns] (no tz)
    df['Month'] = df['Month'].dt.tz_localize(None)

    # 3) now cutoff_dt is naive, so this comparison works:
    # df = df[df['Month'] >= cutoff_dt]  

    # 7) Calculate weighted fields
    df['Wtd Credit Period-Calculated'] = df['TOFU (in lacs)'] * df['Credit Period']
    df['Wtd Max Days-Calculated']     = df['TOFU (in lacs)'] * df['Max Days Advanced']
    df['Wtd Act Days-Calculated']     = df['TOFU (in lacs)'] * df['Days Advanced']
    df['Wtd APR']                     = df['TOFU (in lacs)'] * df['APR']
    df['Wtd Buyer Rev Share']         = df['TOFU (in lacs)'] * df['Buyer Revenue Share (in lacs)']

    # 8) Drop timezone info for Excel compatibility
    for col in df.select_dtypes(include=["datetimetz"]).columns:
        df[col] = df[col].dt.tz_convert(None)

    # 9) Write to Excel
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"cashflo_metrics_{months_back}m.xlsx")
    df.to_excel(out_path, index=False)

    # 10) Clean up
    engine.dispose()
    return out_path
