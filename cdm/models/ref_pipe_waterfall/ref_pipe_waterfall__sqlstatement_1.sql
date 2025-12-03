{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default2"
  })
}}

WITH gsmbs_raw_source AS (

  SELECT * 
  
  FROM {{ source('waterfall_harmonizer_cdm', 'gsmbs_raw_source') }}

),

sqlstatement_1 AS (

  -- Databricks SQL version
  WITH CTE_Level_0 AS (
  
    SELECT 
      `4506-T Indicator` AS `4506T Flag`,
      `Supplemental Field: COVID FB End Dt` AS `AM: Plan End Date`,
      `Supplemental Field: COVID FB Start Dt` AS `AM: Plan Start Date`,
      `Interest Type Indicator` AS `ARM Flag`,
      `Index Type` AS `ARM Index`,
      `Initial Fixed Rate Period` AS `ARM Initial Fixed Rate Period`,
      `Initial Interest Rate Cap (Change Up)` AS `ARM Initial Rate Adj Cap`,
      `ARM Look-back Days` AS `ARM Lookback Days`,
      `Gross Margin` AS `ARM Margin`,
      `Lifetime Maximum Rate (Ceiling)` AS `ARM Max Rate`,
      `Lifetime Minimum Rate (Floor)` AS `ARM Min Rate`,
      `Subsequent Payment Reset Period` AS `ARM Payment Adj Frequency`,
      `Subsequent Interest Rate Cap (Change Up)` AS `ARM Periodic Cap`,
      `Subsequent Interest Rate Reset Period` AS `ARM Rate Adj Frequency`,
      `ARM Round Factor` AS `ARM Round Factor`,
      `ARM Round Flag` AS `ARM Round Flag`,
      `As of Date` AS `Acq Cutoff Date`,
      `Supplemental Field: COVID FB Active Flag` AS `Active Forbearance Flag`,
      `Supplemental Field: Initial HELOC Draw Amount` AS `Amount First Disbursement`,
      `As of Date` AS `As Of Date`,
      `Borrower Asset Verification` AS `Borr 1 Asset Verification`,
      `Most Recent Primary Borrower FICO` AS `Borr 1 Credit Score`,
      `Most Recent FICO Date` AS `Borr 1 Credit Score Date`,
      `Borrower Employment Verification` AS `Borr 1 Employment Verification`,
      `Borrower Income Verification Level` AS `Borr 1 Income Verification`,
      `Length of Employment: Borrower` AS `Borr 1 Length of Employment`,
      `Primary Borrower Other Income` AS `Borr 1 Other Income`,
      `Self-employment Flag` AS `Borr 1 Self-Employment Flag`,
      `Primary Borrower Wage Income` AS `Borr 1 Wage Income`,
      `Co-Borrower Asset Verification` AS `Borr 2 Asset Verification`,
      `Most Recent Co-Borrower FICO` AS `Borr 2 Credit Score`,
      `Co-Borrower Employment Verification` AS `Borr 2 Employment Verification`,
      `Co-Borrower Income Verification` AS `Borr 2 Income Verification`,
      `Length of Employment: Co-Borrower` AS `Borr 2 Length of Employment`,
      `Co-Borrower Other Income` AS `Borr 2 Other Income`,
      `Co-Borrower Wage Income` AS `Borr 2 Wage Income`,
      `Primary Borrower ID` AS `BorrowerID`,
      `Broker Indicator` AS `Broker Flag`,
      `Buy Down Period` AS `Buydown Period`,
      `Cash Out Amount` AS `Cash Out Amount`,
      `MI Certificate Number` AS `Claim Certificate Number`,
      `Supplemental Field: Existing Corporate Advance` AS `Corp Advance Recoverable Balance`,
      `Credit Line Usage Ratio` AS `Credit Line Usage Ratio`,
      `Credit Report: Longest Trade Line` AS `Credit: Longest Trade Line`,
      `Credit Report: Maximum Trade Line` AS `Credit: Maximum Trade Line`,
      `Credit Report: Number of Trade Lines` AS `Credit: Number of Trade Lines`,
      `Total Deferred Amount` AS `Curr Deferred Balance`,
      `Current Loan Amount` AS `Curr Loan Balance`,
      `Current Interest Rate` AS `Curr Loan Interest Rate`,
      `Maturity Date` AS `Curr Loan Maturity Date`,
      `Current Payment Amount Due` AS `Curr PI Payment`,
      `Paid Through Date` AS `Curr Payment Paid Thru Date`,
      `Current Payment Status` AS `Curr Servicer Delinquency Status`,
      `Junior Mortgage Balance` AS `Current Junior Mortgage Balance`,
      `Current 'Other' Monthly Payment` AS `Current Other Payment`,
      `Current Payment Amount Due` AS `Current Payment Amount Due`,
      `Supplemental Field: TILA Status` AS `DD: TIL Status`,
      `Originator DTI` AS `DTI Ratio`,
      `Updated DTI (Front-end)` AS `DTI Ratio (Front-End)`,
      `Supplemental Field: Documentation Type` AS `Doc Type`,
      `Supplemental Field: Existing Escrow Advance` AS `Escrow Advance Balance`,
      `Escrow Indicator` AS `Escrow Flag`,
      `Covered/High Cost Loan Indicator` AS `High Cost Flag`,
      `Initial Fixed Payment Period` AS `Init Fixed Payment Period`,
      `Initial Minimum Payment` AS `Init Min Payment`,
      `Initial Negative Amortization Recast Period` AS `Init NegAm Recast Period`,
      `Initial Interest Rate Cap (Change Down)` AS `Init Periodic Cap (Down)`,
      `Initial Periodic Payment Cap` AS `Init Periodic Pay Cap`,
      `HELOC Draw Period` AS `LOC Draw Period`,
      `Supplemental Field: HELOC Status` AS `LOC Line Status`,
      `Original Loan Amount` AS `LOC Max Original Credit Line`,
      `Lien Position` AS `Lien Position`,
      `Heloc Indicator` AS `Line of Credit Flag`,
      `Liquid / Cash Reserves` AS `Liquid / Cash Reserves`,
      `Origination Date` AS `Loan Funding Date`,
      `Loan Purpose` AS `Loan Purpose`,
      `MI Certificate Number` AS `MI Cert Num`,
      `Mortgage Insurance Company Name` AS `MI Company`,
      `Mortgage Insurance Percent` AS `MI Coverage Pct`,
      `MI: Lender or Borrower Paid?` AS `MI Type`,
      `Total Capitalized Amount` AS `Mod Capitalized Amount`,
      `Forgiven Interest Amount` AS `Mod Forgiven Interest Amount`,
      `Forgiven Principal Amount` AS `Mod Forgiven Principal Amount`,
      `Supplemental Field: Step Rate 1` AS `Mod Step Rate 1`,
      `Supplemental Field: Step Date 1` AS `Mod Step Start 1`,
      `Modification Effective Payment Date` AS `Modification Date`,
      `Supplemental Field: Mod_flag` AS `Modification Flag`,
      `Monthly Debt All Borrowers` AS `Monthly Debt All Borrowers`,
      `Months Bankruptcy` AS `Months Since BK Discharge`,
      `Months Foreclosure` AS `Months Since FC Sale`,
      `Number of Mortgaged Properties` AS `Multiple Properties Flag`,
      `Negative Amortization Limit` AS `NegAm Limit`,
      `Number of Mortgaged Properties` AS `Number Of Properties`,
      `Total Number of Borrowers` AS `Number of Borrowers`,
      `Number of Modifications` AS `Number of Modifications`,
      `Option ARM Indicator` AS `OptionARM Flag`,
      `Original Amortization Term` AS `Orig Amortization Term`,
      `Amortization Type` AS `Orig Amortization Type`,
      `Original Property Valuation Date` AS `Orig Appraisal Date`,
      `Original Appraised Property Value` AS `Orig Appraisal Value`,
      `Original CLTV` AS `Orig CLTV`,
      `Channel` AS `Orig Channel`,
      `First Payment Date of Loan` AS `Orig First Payment Date`,
      `Original Interest Only Term` AS `Orig Interest Only Term`,
      `Junior Mortgage Balance` AS `Orig Junior Loan Amount`,
      `Original LTV` AS `Orig LTV`,
      `Original Loan Amount` AS `Orig Loan Amount`,
      `Original Interest Rate` AS `Orig Loan Interest Rate`,
      `Original Term to Maturity` AS `Orig Loan Term`,
      `Occupancy` AS `Orig Occupancy Status`,
      `Original Pledged Assets` AS `Orig Pledged Assets`,
      `Sales Price` AS `Orig Sales Price`,
      `Supplemental Field: Original Qualifying FICO` AS `Original FICO`,
      `Total Origination and Discount Points (in dollars)` AS `Origination Discount Points ($)`,
      `Origination Date` AS `Origination Note Date`,
      `Loan Number` AS `Originator Loan ID`,
      `Originator` AS `Originator Name`,
      `Most Recent 24-month Pay History` AS `Pay History 24 Mths`,
      `Current Payment Status` AS `Performance Status`,
      `Loan Group` AS `Pool`,
      `Pool Insurance Co. Name` AS `Pool Ins Company`,
      `Pool Insurance Stop Loss %` AS `Pool Ins Stop Loss %`,
      `Pre-Modification Initial Interest Rate Change Downward Cap` AS `Pre-Mod Init Periodic Cap`,
      `Pre-Modification I/O Term` AS `Pre-Mod Interest Only Term`,
      `Pre-Modification Interest (Note) Rate` AS `Pre-Mod Interest Rate`,
      `Pre-Modification Next Interest Rate Change Date` AS `Pre-Mod Next Rate Adj Date`,
      `Pre-Modification P&I Payment` AS `Pre-Mod PI Payment`,
      `Pre-Modification Subsequent Interest Rate Cap` AS `Pre-Mod Subseq Periodic Cap`,
      `Prepayment Penalty Calculation` AS `Prepayment Penalty Description`,
      `Prepayment Penalty Hard Term` AS `Prepayment Penalty Hard Term`,
      `Prepayment Penalty Total Term` AS `Prepayment Penalty Period`,
      `City` AS `Property City`,
      `Supplemental Field: Collateral Type` AS `Property Description`,
      `Most Recent Property Value` AS `Property Latest Value ($)`,
      `Most Recent Property Valuation Date` AS `Property Latest Value Date`,
      `Most Recent Property Valuation Type` AS `Property Latest Value Method`,
      `Most Recent AVM Model Name` AS `Property Latest Value Source`,
      `State` AS `Property State`,
      `Property Type` AS `Property Type`,
      `Postal Code` AS `Property Zip`,
      `Qualification Method` AS `Qualification Method`,
      `Relocation Loan Indicator` AS `Relocation Flag`,
      `Self-employment Flag` AS `Self-Employment Flag`,
      `Supplemental Field: Seller` AS `Seller`,
      `Senior Loan Amount(s)` AS `Senior Lien Current Balance`,
      `Hybrid Period of Most Senior Lien (in months)` AS `Senior Lien Hybrid Period`,
      `Supplemental Field: Senior Lien Note Rate` AS `Senior Lien Loan Interest Rate`,
      `Loan Type of Most Senior Lien` AS `Senior Lien Loan Type`,
      `Neg Am Limit of Most Senior Lien` AS `Senior Lien Neg Am Limit`,
      `Origination Date of Most Senior Lien` AS `Senior Lien Origination Note Date`,
      `Primary Servicer` AS `Servicer Name`,
      `Subsequent Negative Amortization Recast Period` AS `Subseq NegAm Recast Period`,
      `Subsequent Interest Rate (Change Down)` AS `Subseq Periodic Cap (Down)`,
      `Subsequent Periodic Payment Cap` AS `Subseq Periodic Pay Cap`,
      `Servicing Advance Methodology` AS `Svc Advance Method`,
      `Servicing Fee-Flat Dollar` AS `Svc Fee Amt ($)`,
      `Servicing Fee-Percentage` AS `Svc Fee Rate (%)`,
      `All Borrower Total Income` AS `Total Combined Monthly Income`,
      `All Borrower Wage Income` AS `Total Combined Wage Income`,
      `Years in Home` AS `Years in Home`
    
    FROM `gsmbs_raw_source`
  
  ),
  
  CTE_Level_1 AS (
  
    SELECT 
      *,
      -- If ARM Flag = 0 => NULL else add months (using ARM Initial Fixed Rate Period or ARM Rate Adj Frequency)
      CASE
        WHEN `ARM Flag` = 0
          THEN NULL
        ELSE add_months(
          to_date(`Orig First Payment Date`), 
          COALESCE(CAST(`ARM Initial Fixed Rate Period` AS INT), CAST(`ARM Rate Adj Frequency` AS INT)))
      END AS `ARM First Rate Adj Date`,
      -- Current available LOC
      `LOC Max Original Credit Line` - `Curr Loan Balance` AS `Curr Available Line of Credit`,
      -- Current CLTV (calc) with nullif for divide-by-zero safety
      (
        COALESCE(`Curr Loan Balance`, 0)
        + COALESCE(`Senior Lien Current Balance`, 0)
        + COALESCE(`Current Junior Mortgage Balance`, 0)
      )
      / NULLIF(`Property Latest Value ($)`, 0) AS `Curr CLTV (Calc)`,
      CASE
        WHEN `Orig Interest Only Term` IS NOT NULL AND CAST(`Orig Interest Only Term` AS DOUBLE) > 0
          THEN 1
        ELSE 0
      END AS `Curr Interest Only Flag`,
      COALESCE(`Current Junior Mortgage Balance`, 0) / NULLIF(`Property Latest Value ($)`, 0) AS `Curr Jr LTV (Calc)`,
      COALESCE(`Curr Loan Balance`, 0) / `Property Latest Value ($)` AS `Curr LTV`,
      COALESCE(`Curr Loan Balance`, 0) / COALESCE(`Property Latest Value ($)`, 0) AS `Curr LTV (Calc)`,
      `Orig Occupancy Status` AS `Curr Occupancy Status`,
      -- next due date one month after paid-thru
      add_months(to_date(`Curr Payment Paid Thru Date`), 1) AS `Curr Payment Next Due Date`,
      COALESCE(`Senior Lien Current Balance`, 0) / NULLIF(`Property Latest Value ($)`, 0) AS `Curr Sr LTV (Calc)`,
      COALESCE(`Borr 1 Credit Score`, `Borr 2 Credit Score`) AS `Current FICO`,
      `Borr 1 Credit Score Date` AS `Current FICO Date`,
      CASE
        WHEN lower(COALESCE(`MI Type`, '')) LIKE '%fha%'
          THEN 1
        ELSE 0
      END AS `FHA Flag`,
      CASE
        WHEN lower(COALESCE(`Doc Type`, '')) LIKE '%f%ll%'
          THEN 1
        ELSE 0
      END AS `Full Doc Flag`,
      -- Loan Age: floor(months_between(AsOfDate, OrigFirstPaymentDate)) + 1, not negative
      GREATEST(0, CAST(FLOOR(months_between(to_date(`As Of Date`), to_date(`Orig First Payment Date`))) AS INT) + 1) AS `Loan Age`,
      CASE
        WHEN `Orig Amortization Term` > `Orig Loan Term`
          THEN 1
        ELSE 0
      END AS `Orig Balloon Flag`,
      CASE
        WHEN CAST(`Orig Interest Only Term` AS DOUBLE) > 0
          THEN add_months(to_date(`Orig First Payment Date`), CAST(`Orig Interest Only Term` AS INT))
        ELSE NULL
      END AS `Orig Interest Only Exp Date`,
      CASE
        WHEN CAST(`Orig Interest Only Term` AS DOUBLE) > 0
          THEN 1
        ELSE 0
      END AS `Orig Interest Only Flag`,
      -- Orig Jr LTV: divide by the lesser of sales price / appraisal value (handle nulls)
      COALESCE(`Orig Junior Loan Amount`, 0)
      / CASE
          WHEN `Orig Sales Price` IS NOT NULL AND `Orig Appraisal Value` IS NOT NULL
            THEN CASE
              WHEN `Orig Sales Price` < `Orig Appraisal Value`
                THEN `Orig Sales Price`
              ELSE `Orig Appraisal Value`
            END
          WHEN `Orig Sales Price` IS NOT NULL
            THEN `Orig Sales Price`
          ELSE `Orig Appraisal Value`
        END AS `Orig Jr LTV`,
      COALESCE(`Orig Junior Loan Amount`, 0)
      / NULLIF(
          CASE
            WHEN `Orig Sales Price` IS NOT NULL AND `Orig Appraisal Value` IS NOT NULL
              THEN CASE
                WHEN `Orig Sales Price` < `Orig Appraisal Value`
                  THEN `Orig Sales Price`
                ELSE `Orig Appraisal Value`
              END
            WHEN `Orig Sales Price` IS NOT NULL
              THEN `Orig Sales Price`
            ELSE `Orig Appraisal Value`
          END, 
          0) AS `Orig Jr LTV (Calc)`,
      COALESCE(`Orig Loan Amount`, 0)
      / NULLIF(
          CASE
            WHEN `Orig Sales Price` < `Orig Appraisal Value` OR `Orig Appraisal Value` IS NULL
              THEN `Orig Sales Price`
            WHEN `Orig Appraisal Value` < `Orig Sales Price` OR `Orig Sales Price` IS NULL
              THEN `Orig Appraisal Value`
            ELSE `Orig Sales Price`
          END, 
          0) AS `Orig LTV (Calc)`,
      CAST(FLOOR(months_between(to_date(`Curr Loan Maturity Date`), to_date(`Orig First Payment Date`))) AS INT) AS `Orig Loan Term (Calc to Curr Maturity)`,
      CASE
        WHEN `Prepayment Penalty Period` > 0
          THEN 1
        ELSE 0
      END AS `Orig Prepayment Penalty Flag`,
      -- Pay History Current for 24: if string exists and removing all '0' leaves empty => 1 else 0 (else NULL)
      CASE
        WHEN LENGTH(CAST(`Pay History 24 Mths` AS STRING)) > 0
          THEN CASE
            WHEN LENGTH(REPLACE(CAST(`Pay History 24 Mths` AS STRING), '0', '')) = 0
              THEN 1
            ELSE 0
          END
        ELSE NULL
      END AS `Pay History Current for 24`,
      CASE
        WHEN `Modification Flag` = 1
          THEN `Curr Loan Maturity Date`
        ELSE NULL
      END AS `Post-Mod Maturity Date`,
      -- Prepayment Penalty Exp Date
      add_months(to_date(`Orig First Payment Date`), CAST(`Prepayment Penalty Period` AS INT)) AS `Prepayment Penalty Exp Date`,
      CASE
        WHEN `ARM Flag` = 1
          THEN 'ARM'
        ELSE 'Fixed'
      END AS `Rate Type`,
      -- Remaining amortization NPER calc using ln; ensure numeric casts
      (
        ln(
          1
          - (
              COALESCE(`Curr Loan Balance`, 0)
              * (COALESCE(`Curr Loan Interest Rate`, 0) / 12.0)
              / NULLIF(`Curr PI Payment`, 0)
            ))
        / ln(1 + (COALESCE(`Curr Loan Interest Rate`, 0) / 12.0))
      )
      * -1 AS `Remaining Amortization Term (NPER Calc)`,
      CAST(FLOOR(months_between(to_date(`Curr Loan Maturity Date`), to_date(`As Of Date`))) AS INT) AS `Remaining Loan Term (Calc to Curr Maturity)`,
      `Orig Loan Amount` AS `Rvs Init Advance Amount`,
      -- Seller Product Type: construct strings with concat
      CASE
        WHEN CAST(`ARM Initial Fixed Rate Period` AS DOUBLE) > 0
          THEN concat('HY', CAST(ROUND(CAST(`ARM Initial Fixed Rate Period` AS DOUBLE) / 12.0, 0) AS INT), '/1')
        ELSE concat(
          CASE
            WHEN `Orig Amortization Type` = 1
              THEN 'FIX'
            ELSE 'ARM'
          END, 
          CAST(COALESCE(ROUND(CAST(`Orig Loan Term` AS DOUBLE) / 60.0, 0) * 5, 30) AS INT))
      END AS `Seller Product Type`,
      CASE
        WHEN lower(COALESCE(`MI Type`, '')) LIKE '%usda%'
          THEN 1
        ELSE 0
      END AS `USDA Flag`,
      CASE
        WHEN lower(COALESCE(`MI Type`, '')) LIKE '%va%'
          THEN 1
        ELSE 0
      END AS `VA Flag`
    
    FROM CTE_Level_0
  
  ),
  
  CTE_Level_2 AS (
  
    SELECT 
      *,
      -- ARM Remaining Fixed Rate Period: ((months_between(ARM First Rate Adj Date, Orig First Payment Date) floored)+1) - Loan Age, else 0
      CASE
        WHEN (
          CAST(FLOOR(months_between(to_date(`ARM First Rate Adj Date`), to_date(`Orig First Payment Date`))) AS INT)
          + 1
        )
        - `Loan Age` > 0
          THEN (
            CAST(FLOOR(months_between(to_date(`ARM First Rate Adj Date`), to_date(`Orig First Payment Date`))) AS INT)
            + 1
          )
          - `Loan Age`
        ELSE 0
      END AS `ARM Remaining Fixed Rate Period`,
      -- Days Delinquent: datediff(AsOfDate, CurrPaymentNextDueDate) + 30 if positive else 0 (approximation of original logic)
      CASE
        WHEN datediff(to_date(`As Of Date`), to_date(`Curr Payment Next Due Date`)) + 30 > 0
          THEN datediff(to_date(`As Of Date`), to_date(`Curr Payment Next Due Date`)) + 30
        ELSE 0
      END AS `Days Delinquent`,
      `Post-Mod Maturity Date` AS `Orig Loan Maturity Date`,
      CAST(FLOOR(months_between(to_date(`Curr Payment Next Due Date`), to_date(`Curr Payment Paid Thru Date`))) AS INT) AS `Payment Frequency`,
      `Orig Amortization Term` - `Loan Age` AS `Remaining Amortization Term`,
      CASE
        WHEN (`Orig Interest Only Term` - `Loan Age`) > 0
          THEN (`Orig Interest Only Term` - `Loan Age`)
        ELSE 0
      END AS `Remaining Interest Only Term`,
      `Orig Loan Term` - CASE
          WHEN `Loan Age` > 0
            THEN `Loan Age`
          ELSE 0
        END AS `Remaining Loan Term`,
      CAST(FLOOR(
        months_between(
          to_date(`Prepayment Penalty Exp Date`), 
          GREATEST(to_date(`As Of Date`), to_date(`Orig First Payment Date`)))) AS INT) AS `Remaining Prepayment Penalty Period`
    
    FROM CTE_Level_1
  
  ),
  
  CTE_Level_3 AS (
  
    SELECT 
      *,
      CASE
        WHEN `Remaining Amortization Term` > `Remaining Loan Term (Calc to Curr Maturity)`
          THEN 1
        ELSE 0
      END AS `Curr Balloon Flag`,
      CAST(FLOOR(months_between(to_date(`Orig Loan Maturity Date`), to_date(`Orig First Payment Date`))) AS INT) AS `Orig Loan Term (Calc to Orig Maturity)`,
      CAST(FLOOR(months_between(to_date(`Orig Loan Maturity Date`), to_date(`As Of Date`))) AS INT) AS `Remaining Loan Term (Calc to Orig Maturity)`
    
    FROM CTE_Level_2
  
  )
  
  SELECT *
  
  FROM CTE_Level_3

)

SELECT *

FROM sqlstatement_1
