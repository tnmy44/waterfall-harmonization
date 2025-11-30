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

as_of_date_yyyymmdd_reformat AS (

  SELECT 
    `Vantagescore Date`,
    `Origination Date`,
    `Modification Effective Payment Date`,
    DATE_FORMAT(`As of Date`, 'yyyyMMdd') AS `As of Date`,
    `Maturity Date`,
    `Most Recent FICO Date`,
    `Original Property Valuation Date`,
    `Pre-Modification Next Interest Rate Change Date`,
    `Most Recent Property Valuation Date`,
    `Origination Date of Most Senior Lien`,
    `Paid Through Date`,
    `Original Interest Rate`,
    `First Payment Date of Loan`,
    `Hybrid Period of Most Senior Lien (in months)`,
    `Initial Fixed Rate Period`,
    `Initial Interest Rate Cap (Change Up)`,
    `Initial Interest Rate Cap (Change Down)`,
    `Most Recent AVM Confidence Score`,
    `Original AVM Confidence Score`,
    `Original Term to Maturity`,
    `Years in Home`,
    `Initial Fixed Payment Period`,
    `Pre-Modification Initial Interest Rate Change Downward Cap`,
    `Months Bankruptcy`,
    `Most Recent 24-month Pay History`,
    `Current Interest Rate`,
    `Original LTV`,
    `Original Pledged Assets`,
    `Pre-Modification Interest (Note) Rate`,
    `Updated DTI (Back-end)`,
    `Subsequent Interest Rate Cap (Change Up)`,
    `Subsequent Interest Rate Reset Period`,
    `Buy Down Period`,
    `MI Certificate Number`,
    `Original Loan Amount`,
    `Original Amortization Term`,
    `Initial Negative Amortization Recast Period`,
    `Original Interest Only Term`,
    `Subsequent Interest Rate (Change Down)`,
    `Forgiven Interest Amount`,
    `Pre-Modification Subsequent Interest Rate Cap`,
    `Most Recent FICO Method`,
    `Most Recent Vantagescore Method`,
    `Negative Amortization Limit`,
    `Postal Code`,
    `Subsequent Negative Amortization Recast Period`,
    `Updated DTI (Front-end)`,
    `Senior Loan Amount(s)`,
    `Borrower Income Verification Level`,
    `Most Recent AVM Model Name`,
    `Months Foreclosure`,
    `Junior Mortgage Balance`,
    `Most Recent Property Valuation Type`,
    `Most Recent Primary Borrower FICO`,
    `Lifetime Maximum Rate (Ceiling)`,
    `Length of Employment: Borrower`,
    `Borrower Asset Verification`,
    `HELOC Draw Period`,
    `Channel`,
    `Initial Minimum Payment Reset Period`,
    `Monthly Debt All Borrowers`,
    `Initial Periodic Payment Cap`,
    `Originator DTI`,
    `Subsequent Payment Reset Period`,
    `Most Recent Property Value`
  
  FROM gsmbs_raw_source

)

SELECT *

FROM as_of_date_yyyymmdd_reformat
