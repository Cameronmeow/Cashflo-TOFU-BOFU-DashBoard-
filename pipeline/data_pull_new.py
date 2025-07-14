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
    today = datetime.now(timezone.utc)
    window_end = datetime(today.year, today.month, 1, tzinfo=timezone.utc)
    window_start = window_end - timedelta(days=30 * months_back)

    # Convert to string format for SQL injection
    window_start_str = window_start.date().isoformat()
    window_end_str = window_end.date().isoformat()
    # 3) Create database engine
    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DB}",
        connect_args={"options": "-c statement_timeout=0"}
    )

    # 4) Define the SQL query with dynamic cutoff filter
    QUERY = dedent(f"""
 -- Final vendor‐level summary across 6M and 12M windows, listing buyers and all KPIs
WITH

-- 1) Invoice‐ and EPR‐level revenue summary
DateParams AS (
  SELECT 
    DATE '{window_start_str}' AS window_start,
    DATE '{window_end_str}'   AS window_end,
    DATE_TRUNC('month', DATE '{window_end_str}') AS window_end_month
),
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
  GROUP BY 1, 2
),

-- 2) Partner×month metrics (all history)
VendorData AS (
  SELECT
    DATE_TRUNC('month', COALESCE(i."createdAt", epriAgg."activatedOn")) AS "Month",
    p."id"                                        AS "Partner ID",
    SUM(i."amount")                               AS "TOFU",
    MAX(epriAgg."requestPlacedAmount")            AS "BOFU",
    -- Credit Period
    SUM(
      CAST(
        DATE_PART('day',
          DATE_TRUNC('day', i."dueDateAtUtc"::timestamp)
          - DATE_TRUNC('day', i."generatedAtUtc"::timestamp)
        ) AS numeric
      ) * i."amount"
    ) / NULLIF(SUM(i."amount"), 0)                 AS "Credit Period",
    -- Max Days Advanced
    SUM(
      (
        DATE_PART('day',
          DATE_TRUNC('day', i."dueDateAtUtc"::timestamp AT TIME ZONE 'UTC')
          - DATE_TRUNC('day', i."createdAt"::timestamp AT TIME ZONE 'UTC')
        )
        - p."settlementDays"
      ) * i."amount"
    ) / NULLIF(SUM(i."amount"), 0)                 AS "Max Days Advanced",
    MAX(epriAgg."weightedDaysAdvanced")           AS "Days Advanced",
    MAX(epriAgg."totalEffectiveDiscount")         AS "Effective Discount",
    -- Acceleration Rate
    (COALESCE(MAX(epriAgg."requestPlacedAmount"),0) / NULLIF(SUM(i."amount"),0)) * 100
                                                  AS "Acc Rate",
    -- APR
    ((MAX(epriAgg."totalEffectiveDiscount") / NULLIF(MAX(epriAgg."requestPlacedAmount"),0))
      * (365 / NULLIF(MAX(epriAgg."weightedDaysAdvanced"),0))
    ) * 100                                       AS "APR"
  FROM (
    SELECT DISTINCT ON (
      trim(i1."invoiceNumber"),
      i1."partnerId",
      CASE
        WHEN extract(month FROM i1."generatedAtUtc") >= 4
          THEN extract(year FROM i1."generatedAtUtc")
        ELSE extract(year FROM i1."generatedAtUtc") - 1
      END
    ) i1.*
    FROM discounting."Invoice" i1
    WHERE i1."amount" > 0
      AND DATE_TRUNC('day', i1."dueDateAtUtc") > DATE_TRUNC('day', i1."createdAt") + INTERVAL '1 day'
      AND DATE_TRUNC('day', i1."createdAt") < DATE_TRUNC('day', i1."generatedAtUtc") + INTERVAL '180 day'
    ORDER BY
      trim(i1."invoiceNumber"),
      i1."partnerId",
      CASE
        WHEN extract(month FROM i1."generatedAtUtc") >= 4
          THEN extract(year FROM i1."generatedAtUtc")
        ELSE extract(year FROM i1."generatedAtUtc") - 1
      END,
      i1."createdAt" ASC
  ) i
  FULL JOIN (
    SELECT
      epr."partnerId",
      DATE_TRUNC('month', epr."activatedOn")    AS "activatedOn",
      SUM(i2."amount")                          AS "requestPlacedAmount",
      SUM(i2."amount" * COALESCE(epri2."daysAdvanced",0)) / SUM(i2."amount")
                                                AS "weightedDaysAdvanced",
      SUM(epri2."effectiveDiscount")            AS "totalEffectiveDiscount"
    FROM discounting."EarlyPaymentRequest"        epr
    JOIN discounting."EarlyPaymentRequestInvoice" epri2
      ON epr."id" = epri2."eprId"
    JOIN discounting."Invoice"                  i2
      ON i2."id" = epri2."invoiceId"
    WHERE epri2."eprInvoiceStatusId" IN (0,1,2)
    GROUP BY 1, 2
  ) epriAgg
    ON i."partnerId" = epriAgg."partnerId"
   AND DATE_TRUNC('month', COALESCE(i."createdAt", epriAgg."activatedOn")) = epriAgg."activatedOn"
  JOIN tenant."Partner" p
    ON p."id" = COALESCE(i."partnerId", epriAgg."partnerId")
  GROUP BY 1, 2
),

-- 3) Roll up to vendor×month, include all needed fields
VendorMonthly AS (
  SELECT
    p."vendorOrgId"         AS "Vendor ID",
    p."buyerOrgId"          AS "Buyer ID",
    DATE_TRUNC('month', COALESCE(vd."Month", vd."Month")) AS "Month",
    vd."TOFU",
    vd."BOFU",
    vd."Credit Period",
    vd."Max Days Advanced",
    vd."Days Advanced",
    vd."Effective Discount" AS "ED",
    vd."Acc Rate",
    vd."APR",
    rs."Platform Fee",
    rs."Buyer Revenue Share"
  FROM VendorData vd
  JOIN tenant."Partner" p
    ON p."id" = vd."Partner ID"
  LEFT JOIN RequestSummary rs
    ON rs."Partner ID" = vd."Partner ID"
   AND rs."Month"      = vd."Month"
),

-- 4) Rolling sums & counts
VendorStats AS (
  SELECT
    vm.*,
    dp.window_start,
    dp.window_end,

    -- 6M Aggregates (within window)
    COALESCE(SUM(CASE WHEN vm."Month" BETWEEN dp.window_start AND dp.window_end THEN vm."TOFU" END) OVER w6, 0) AS sum_tofu_amt_6,
    COALESCE(SUM(CASE WHEN vm."Month" BETWEEN dp.window_start AND dp.window_end THEN vm."BOFU" END) OVER w6, 0) AS sum_bofu_amt_6,

    COALESCE(SUM(CASE WHEN vm."Month" BETWEEN dp.window_start AND dp.window_end AND vm."TOFU">0 THEN 1 ELSE 0 END) OVER w6, 0) AS count_tofu_6,
    COALESCE(SUM(CASE WHEN vm."Month" BETWEEN dp.window_start AND dp.window_end AND vm."BOFU">0 THEN 1 ELSE 0 END) OVER w6, 0) AS count_bofu_6,

    -- 12M Aggregates (within window)
    COALESCE(SUM(CASE WHEN vm."Month" BETWEEN dp.window_start AND dp.window_end THEN vm."TOFU" END) OVER w12, 0) AS sum_tofu_amt_12,
    COALESCE(SUM(CASE WHEN vm."Month" BETWEEN dp.window_start AND dp.window_end THEN vm."BOFU" END) OVER w12, 0) AS sum_bofu_amt_12,

    COALESCE(SUM(CASE WHEN vm."Month" BETWEEN dp.window_start AND dp.window_end AND vm."TOFU">0 THEN 1 ELSE 0 END) OVER w12, 0) AS count_tofu_12,
    COALESCE(SUM(CASE WHEN vm."Month" BETWEEN dp.window_start AND dp.window_end AND vm."BOFU">0 THEN 1 ELSE 0 END) OVER w12, 0) AS count_bofu_12,

    -- BOFU before current month but only within window
    COALESCE(
      MAX(CASE
             WHEN vm."BOFU">0 AND vm."Month" BETWEEN dp.window_start AND dp.window_end
             THEN vm."Month"
           END) OVER (
             PARTITION BY vm."Vendor ID"
             ORDER BY vm."Month"
             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ),
      DATE '1900-01-01'
    ) AS last_bofu_before

  FROM VendorMonthly vm
  CROSS JOIN DateParams dp
  WHERE vm."Month" BETWEEN dp.window_start AND dp.window_end

  WINDOW
    w6  AS (PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
            RANGE BETWEEN INTERVAL '5 months' PRECEDING AND CURRENT ROW),
    w12 AS (PARTITION BY vm."Vendor ID" ORDER BY vm."Month"
            RANGE BETWEEN INTERVAL '11 months' PRECEDING AND CURRENT ROW)
),
-- 5) Vendor‐level categories & date markers
VendorCategories AS (
  SELECT
    vs."Vendor ID",
    vs."Month",
    dp.window_start,
    dp.window_end,
    dp.window_end_month,

MIN(CASE
      WHEN vs."TOFU" > 0 AND vs."Month" BETWEEN dp.window_start AND dp.window_end
      THEN vs."Month"
    END) OVER w_v AS first_tofu_month,

MAX(CASE
      WHEN vs."TOFU" > 0 AND vs."Month" BETWEEN dp.window_start AND dp.window_end
      THEN vs."Month"
    END) OVER w_v AS last_tofu_month,

MIN(CASE
      WHEN vs."BOFU" > 0 AND vs."Month" BETWEEN dp.window_start AND dp.window_end
      THEN vs."Month"
    END) OVER w_v AS first_bofu_month,

MAX(CASE
      WHEN vs."BOFU" > 0 AND vs."Month" BETWEEN dp.window_start AND dp.window_end
      THEN vs."Month"
    END) OVER w_v AS last_bofu_month,

    -- TOFU Category (6M)
    CASE
      WHEN MIN(CASE WHEN vs."TOFU" > 0 THEN vs."Month" END) OVER w_v = dp.window_end_month
           AND MIN(CASE WHEN vs."TOFU" > 0 THEN vs."Month" END) OVER w_v >= dp.window_start
      THEN 'TOFU New'
      WHEN vs.count_tofu_6 >= 5 THEN 'Regular'
      WHEN vs.count_tofu_6 BETWEEN 3 AND 4 THEN 'Sporadic'
      WHEN vs.count_tofu_6 BETWEEN 1 AND 2 THEN 'Low'
      ELSE 'None'
    END AS tofu_cat_6m,

    -- BOFU Category (6M)
    CASE
      WHEN MIN(CASE WHEN vs."BOFU" > 0 THEN vs."Month" END) OVER w_v = dp.window_end_month
           AND MIN(CASE WHEN vs."BOFU" > 0 THEN vs."Month" END) OVER w_v >= dp.window_start
      THEN 'BOFU New'
      WHEN vs.count_bofu_6 = 0 THEN 'Never Transacted'
      WHEN (SUM(CASE WHEN vs."TOFU" > 0 THEN 1 ELSE 0 END) OVER w3_6 = 3
         AND SUM(CASE WHEN vs."BOFU" > 0 THEN 1 ELSE 0 END) OVER w3_6 = 0)
        THEN 'Churned'
      WHEN (SUM(CASE WHEN vs."TOFU" > 0 THEN 1 ELSE 0 END) OVER w2_6 = 2
         AND SUM(CASE WHEN vs."BOFU" > 0 THEN 1 ELSE 0 END) OVER w2_6 = 0)
        THEN 'At Risk'
      WHEN vs.sum_bofu_amt_6::numeric / NULLIF(vs.sum_tofu_amt_6, 0) >= 0.8 THEN 'High'
      WHEN vs.sum_bofu_amt_6::numeric / NULLIF(vs.sum_tofu_amt_6, 0) >= 0.5 THEN 'Med'
      WHEN vs.sum_bofu_amt_6::numeric / NULLIF(vs.sum_tofu_amt_6, 0) > 0 THEN 'Low'
      ELSE 'Never Transacted'
    END AS bofu_cat_6m,

    -- TOFU Category (12M)
    CASE
      WHEN MIN(CASE WHEN vs."TOFU" > 0 THEN vs."Month" END) OVER w_v = dp.window_end_month
           AND MIN(CASE WHEN vs."TOFU" > 0 THEN vs."Month" END) OVER w_v >= dp.window_start
      THEN 'TOFU New'
      WHEN vs.count_tofu_12 >= 10 THEN 'Regular'
      WHEN vs.count_tofu_12 BETWEEN 6 AND 9 THEN 'Sporadic'
      WHEN vs.count_tofu_12 BETWEEN 1 AND 5 THEN 'Low'
      ELSE 'None'
    END AS tofu_cat_12m,

    -- BOFU Category (12M)
    CASE
      WHEN MIN(CASE WHEN vs."BOFU" > 0 THEN vs."Month" END) OVER w_v = dp.window_end_month
           AND MIN(CASE WHEN vs."BOFU" > 0 THEN vs."Month" END) OVER w_v >= dp.window_start
      THEN 'BOFU New'
      WHEN vs.count_bofu_12 = 0 THEN 'Never Transacted'
      WHEN vs.sum_bofu_amt_12::numeric / NULLIF(vs.sum_tofu_amt_12, 0) >= 0.8 THEN 'High'
      WHEN vs.sum_bofu_amt_12::numeric / NULLIF(vs.sum_tofu_amt_12, 0) >= 0.5 THEN 'Med'
      WHEN vs.sum_bofu_amt_12::numeric / NULLIF(vs.sum_tofu_amt_12, 0) > 0 THEN 'Low'
      ELSE 'Never Transacted'
    END AS bofu_cat_12m

  FROM VendorStats vs
  CROSS JOIN DateParams dp
  WINDOW
    w_v AS (PARTITION BY vs."Vendor ID"),
    w3_6 AS (PARTITION BY vs."Vendor ID" ORDER BY vs."Month" ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),
    w2_6 AS (PARTITION BY vs."Vendor ID" ORDER BY vs."Month" ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
),
-- 6) Pick latest stats snapshot (last month per vendor)
LatestStats AS (
  SELECT DISTINCT ON (vs."Vendor ID") vs.*
  FROM VendorStats vs
  ORDER BY vs."Vendor ID", vs."Month" DESC
),

-- 7) Final vendor summary
-- 7) Final vendor summary (completed)
VendorSummary AS (
  SELECT
    vendororg."PAN"              AS "PAN",
    vendororg."legalName"        AS "Vendor Name",

    -- Buyers counts & lists
    COUNT(DISTINCT CASE WHEN vm."TOFU">0 THEN vm."Buyer ID" END)            AS "Number of Buyers (TOFU)",
    STRING_AGG(DISTINCT CASE WHEN vm."TOFU">0 THEN buyerorg."legalName" END, ', ')
                                                                           AS "List of Buyers (TOFU)",
    COUNT(DISTINCT CASE WHEN vm."BOFU">0 THEN vm."Buyer ID" END)            AS "Number of Buyers (BOFU)",
    STRING_AGG(DISTINCT CASE WHEN vm."BOFU">0 THEN buyerorg."legalName" END, ', ')
                                                                           AS "List of Buyers (BOFU)",

    -- 6M aggregates in lacs
    ls.count_tofu_6                                AS "TOFU Count (6M)",
    (ls.sum_tofu_amt_6    /100000)::numeric(18,2)  AS "TOFU Amount (6M)",
    ROUND((ls.sum_tofu_amt_6/6.0/100000)::numeric,2) AS "TOFU Monthly Avg (6M)",

    ls.count_bofu_6                                AS "BOFU Count (6M)",
    (ls.sum_bofu_amt_6    /100000)::numeric(18,2)  AS "BOFU Amount (6M)",
    ROUND((ls.sum_bofu_amt_6/6.0/100000)::numeric,2) AS "BOFU Monthly Avg (6M)",

    -- weighted averages in lacs (metric * TOFU / sum_tofu_amt_6 / 100000)
    ROUND((
  SUM(vm."ED")
    FILTER (
      WHERE vm."Month"
        BETWEEN date_trunc('month', ls."Month") - INTERVAL '5 months'
            AND ls."Month"
    )
  / 6.0
  / 100000
)::numeric,2) AS "ED Wtd Avg (6M)",
    ROUND((
  SUM((vm."Platform Fee" + vm."Buyer Revenue Share"))
    FILTER (
      WHERE vm."Month"
        BETWEEN date_trunc('month', ls."Month") - INTERVAL '5 months'
            AND ls."Month"
    )
  / 6.0
  / 100000
)::numeric,2) AS "Revenue Wtd Avg (6M)",

    -- derived KPIs (no lacs division)
    ROUND((ls.sum_bofu_amt_6::numeric / NULLIF(ls.sum_tofu_amt_6,0)*100),2)                AS "Acceleration (6M)",
    ROUND((SUM(vm."Credit Period"*vm."TOFU")FILTER (
      WHERE vm."Month"
        BETWEEN date_trunc('month', ls."Month") - INTERVAL '5 months'
            AND ls."Month"
    ) / NULLIF(ls.sum_tofu_amt_6,0))::numeric,2)     AS "Wtd Avg Credit Period (6M)",
    ROUND((SUM(vm."Max Days Advanced"*vm."TOFU")FILTER (
      WHERE vm."Month"
        BETWEEN date_trunc('month', ls."Month") - INTERVAL '5 months'
            AND ls."Month"
    ) / NULLIF(ls.sum_tofu_amt_6,0))::numeric,2) AS "Wtd Avg Max Days (6M)",
    ROUND((SUM(vm."Days Advanced"*vm."TOFU")FILTER (
      WHERE vm."Month"
        BETWEEN date_trunc('month', ls."Month") - INTERVAL '5 months'
            AND ls."Month"
    ) / NULLIF(ls.sum_tofu_amt_6,0))::numeric,2)     AS "Wtd Avg Actual Days (6M)",
    ROUND((SUM(vm."APR"*vm."TOFU")FILTER (
      WHERE vm."Month"
        BETWEEN date_trunc('month', ls."Month") - INTERVAL '5 months'
            AND ls."Month"
    ) / NULLIF(ls.sum_tofu_amt_6,0))::numeric,2)              AS "Wtd Avg APR (6M)",

    -- 12M aggregates in lacs
    ls.count_tofu_12                               AS "TOFU Count (12M)",
    (ls.sum_tofu_amt_12   /100000)::numeric(18,2)  AS "TOFU Amount (12M)",
    ROUND((ls.sum_tofu_amt_12/12.0/100000)::numeric,2) AS "TOFU Monthly Avg (12M)",

    ls.count_bofu_12                               AS "BOFU Count (12M)",
    (ls.sum_bofu_amt_12   /100000)::numeric(18,2)  AS "BOFU Amount (12M)",
    ROUND((ls.sum_bofu_amt_12/12.0/100000)::numeric,2) AS "BOFU Monthly Avg (12M)",

    -- weighted averages 12M in lacs
    ROUND((
      SUM(vm."ED") FILTER (
        WHERE vm."Month" BETWEEN date_trunc('month',CURRENT_DATE)-INTERVAL '12 months'
                              AND date_trunc('month',CURRENT_DATE)-INTERVAL '1 month'
      )
      / 6.0
      /100000
    )::numeric,2)                                    AS "ED Wtd Avg (12M)",
    ROUND((
      SUM((vm."Platform Fee"+vm."Buyer Revenue Share")) FILTER (
        WHERE vm."Month" BETWEEN date_trunc('month',CURRENT_DATE)-INTERVAL '12 months'
                              AND date_trunc('month',CURRENT_DATE)-INTERVAL '1 month'
      )
      / 6.0
      /100000
    )::numeric,2)                                    AS "Revenue Wtd Avg (12M)",

    -- derived 12M KPIs
    ROUND((ls.sum_bofu_amt_12::numeric / NULLIF(ls.sum_tofu_amt_12,0)*100),2)               AS "Acceleration (12M)",
    
    ROUND((SUM(vm."Credit Period"*vm."TOFU") FILTER (
      WHERE vm."Month" BETWEEN date_trunc('month',CURRENT_DATE)-INTERVAL '12 months'
                            AND date_trunc('month',CURRENT_DATE)-INTERVAL '1 month'
    ) / NULLIF(ls.sum_tofu_amt_12,0))::numeric,2)                                           AS "Wtd Avg Credit Period (12M)",
    ROUND((SUM(vm."Max Days Advanced"*vm."TOFU") FILTER (
      WHERE vm."Month" BETWEEN date_trunc('month',CURRENT_DATE)-INTERVAL '12 months'
                            AND date_trunc('month',CURRENT_DATE)-INTERVAL '1 month'
    ) / NULLIF(ls.sum_tofu_amt_12,0))::numeric,2)                                           AS "Wtd Avg Max Days (12M)",
    ROUND((SUM(vm."Days Advanced"*vm."TOFU") FILTER (
      WHERE vm."Month" BETWEEN date_trunc('month',CURRENT_DATE)-INTERVAL '12 months'
                            AND date_trunc('month',CURRENT_DATE)-INTERVAL '1 month'
    ) / NULLIF(ls.sum_tofu_amt_12,0))::numeric,2)                                           AS "Wtd Avg Actual Days (12M)",
    ROUND((SUM(vm."APR"*vm."TOFU") FILTER (
      WHERE vm."Month" BETWEEN date_trunc('month',CURRENT_DATE)-INTERVAL '12 months'
                            AND date_trunc('month',CURRENT_DATE)-INTERVAL '1 month'
    ) / NULLIF(ls.sum_tofu_amt_12,0))::numeric,2)                                           AS "Wtd Avg APR (12M)",

    -- TOFU/BOFU markers & categories (unchanged)
    vc.first_tofu_month             AS "First TOFU Month",
    vc.last_tofu_month              AS "Last TOFU Month",
    vc.first_bofu_month             AS "First BOFU Month",
    vc.last_bofu_month              AS "Last BOFU Month",
    vc.tofu_cat_6m                  AS "TOFU Category (6M)",
    vc.bofu_cat_6m                  AS "BOFU Category (6M)",
    vc.tofu_cat_12m                 AS "TOFU Category (12M)",
    vc.bofu_cat_12m                 AS "BOFU Category (12M)"

  FROM VendorMonthly vm
  JOIN LatestStats ls
    ON ls."Vendor ID" = vm."Vendor ID"
  JOIN VendorCategories vc
    ON vc."Vendor ID" = vm."Vendor ID"
   AND vc."Month"     = ls."Month"
  JOIN tenant."Organization" vendororg
    ON vendororg."id" = vm."Vendor ID"
  LEFT JOIN tenant."Organization" buyerorg
    ON buyerorg."id"  = vm."Buyer ID"
  CROSS JOIN DateParams dp
  -- NEW:
	WHERE vm."Month" BETWEEN dp.window_start AND dp.window_end


  GROUP BY
    vendororg."PAN", vendororg."legalName",
    ls.count_tofu_6, ls.sum_tofu_amt_6,
    ls.count_bofu_6, ls.sum_bofu_amt_6,
    ls.count_tofu_12, ls.sum_tofu_amt_12,
    ls.count_bofu_12, ls.sum_bofu_amt_12,
    vc.first_tofu_month, vc.last_tofu_month,
    vc.first_bofu_month, vc.last_bofu_month,
    vc.tofu_cat_6m, vc.bofu_cat_6m,
    vc.tofu_cat_12m, vc.bofu_cat_12m
	)
-- 8) Export
SELECT *
FROM VendorSummary
ORDER BY "PAN";


    """)

    # 5) Execute query
    print("Running monthly metrics query …")
    df = pd.read_sql_query(QUERY, engine)

    # # 1) coerce to datetime (this will be tz‐aware because your SQL has UTC timestamps)
    # df['Month'] = pd.to_datetime(df['Month'], utc=True)

    # # 2) strip the timezone off so that dtype is datetime64[ns] (no tz)
    # df['Month'] = df['Month'].dt.tz_localize(None)

    # 3) now cutoff_dt is naive, so this comparison works:
    # df = df[df['Month'] >= cutoff_dt]  

    # 7) Calculate weighted fields
    # df['Wtd Credit Period-Calculated'] = df['TOFU (in lacs)'] * df['Credit Period']
    # df['Wtd Max Days-Calculated']     = df['TOFU (in lacs)'] * df['Max Days Advanced']
    # df['Wtd Act Days-Calculated']     = df['TOFU (in lacs)'] * df['Days Advanced']
    # df['Wtd APR']                     = df['TOFU (in lacs)'] * df['APR']
    # df['Wtd Buyer Rev Share']         = df['TOFU (in lacs)'] * df['Buyer Revenue Share (in lacs)']

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
