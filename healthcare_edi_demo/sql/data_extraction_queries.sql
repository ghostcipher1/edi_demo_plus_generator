-- data_extraction_queries.sql
-- Healthcare EDI Analytics — HealthcareEDIDemo
-- Target: Azure SQL Database | Schemas: dim, fact


-- =============================================================================
-- 1. Total Claims
-- =============================================================================

-- Count of all claims, broken down by status
SELECT
    ClaimStatus,
    COUNT(*)                        AS ClaimCount,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()
                                    AS PctOfTotal
FROM fact.Claims
GROUP BY ClaimStatus
ORDER BY ClaimCount DESC;


-- Monthly claim volume (service date)
SELECT
    d.Year,
    d.Month,
    d.MonthName,
    COUNT(c.ClaimKey)               AS ClaimCount
FROM fact.Claims c
JOIN dim.Date d  ON d.DateKey = c.ServiceDateKey
GROUP BY d.Year, d.Month, d.MonthName
ORDER BY d.Year, d.Month;


-- =============================================================================
-- 2. Total Claim Values
-- =============================================================================

-- Aggregate billed, allowed, paid, and patient responsibility across all claims
SELECT
    SUM(TotalBilledAmount)          AS TotalBilled,
    SUM(TotalAllowedAmount)         AS TotalAllowed,
    SUM(TotalPaidAmount)            AS TotalPaid,
    SUM(PatientResponsibility)      AS TotalPatientResp,
    SUM(TotalPaidAmount)
        / NULLIF(SUM(TotalBilledAmount), 0)
                                    AS CollectionRate
FROM fact.Claims;


-- Claim values by payer
SELECT
    py.PayerName,
    py.PayerCategory,
    COUNT(c.ClaimKey)               AS ClaimCount,
    SUM(c.TotalBilledAmount)        AS TotalBilled,
    SUM(c.TotalPaidAmount)          AS TotalPaid,
    SUM(c.TotalPaidAmount)
        / NULLIF(SUM(c.TotalBilledAmount), 0)
                                    AS CollectionRate
FROM fact.Claims c
JOIN dim.Payer py  ON py.PayerKey = c.PayerKey
GROUP BY py.PayerKey, py.PayerName, py.PayerCategory
ORDER BY TotalBilled DESC;


-- Claim values by provider
SELECT
    pr.LastName + ', ' + pr.FirstName   AS ProviderName,
    pr.Specialty,
    COUNT(c.ClaimKey)                   AS ClaimCount,
    SUM(c.TotalBilledAmount)            AS TotalBilled,
    SUM(c.TotalPaidAmount)              AS TotalPaid,
    AVG(c.TotalBilledAmount)            AS AvgClaimValue
FROM fact.Claims c
JOIN dim.Provider pr  ON pr.ProviderKey = c.ProviderKey
GROUP BY pr.ProviderKey, pr.LastName, pr.FirstName, pr.Specialty
ORDER BY TotalBilled DESC;


-- =============================================================================
-- 3. Denial Percentage
-- =============================================================================

-- Overall denial rate
SELECT
    COUNT(*)                            AS TotalClaims,
    SUM(CASE WHEN ClaimStatus = 'Denied' THEN 1 ELSE 0 END)
                                        AS DeniedClaims,
    SUM(CASE WHEN ClaimStatus = 'Denied' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0)           AS DenialPct
FROM fact.Claims;


-- Denial rate by payer — surfaces high-denial payers
SELECT
    py.PayerName,
    py.PayerCategory,
    COUNT(c.ClaimKey)                   AS TotalClaims,
    SUM(CASE WHEN c.ClaimStatus = 'Denied' THEN 1 ELSE 0 END)
                                        AS DeniedClaims,
    SUM(CASE WHEN c.ClaimStatus = 'Denied' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(c.ClaimKey), 0)  AS DenialPct,
    SUM(c.TotalBilledAmount)
        - SUM(c.TotalPaidAmount)        AS RevenueLost
FROM fact.Claims c
JOIN dim.Payer py  ON py.PayerKey = c.PayerKey
GROUP BY py.PayerKey, py.PayerName, py.PayerCategory
ORDER BY DenialPct DESC;


-- Denial rate by provider specialty
SELECT
    pr.Specialty,
    COUNT(c.ClaimKey)                   AS TotalClaims,
    SUM(CASE WHEN c.ClaimStatus = 'Denied' THEN 1 ELSE 0 END)
                                        AS DeniedClaims,
    SUM(CASE WHEN c.ClaimStatus = 'Denied' THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(c.ClaimKey), 0)  AS DenialPct
FROM fact.Claims c
JOIN dim.Provider pr  ON pr.ProviderKey = c.ProviderKey
GROUP BY pr.Specialty
ORDER BY DenialPct DESC;


-- Denial breakdown by CARC (Claim Adjustment Reason Code)
SELECT
    p.CARC,
    p.DenialCategory,
    COUNT(p.PaymentKey)                 AS DenialCount,
    SUM(p.BilledAmount)                 AS BilledAtDenial
FROM fact.Payments p
WHERE p.PaidAmount = 0
   OR p.DenialCategory IS NOT NULL
GROUP BY p.CARC, p.DenialCategory
ORDER BY DenialCount DESC;


-- =============================================================================
-- 4. Average Days to Payment
-- =============================================================================

-- Overall average and distribution of payment speed
SELECT
    AVG(CAST(DaysToPayment AS FLOAT))   AS AvgDaysToPayment,
    MIN(DaysToPayment)                  AS MinDays,
    MAX(DaysToPayment)                  AS MaxDays,
    -- approximate median via percentile
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DaysToPayment)
        OVER ()                         AS MedianDays,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY DaysToPayment)
        OVER ()                         AS P90Days
FROM fact.Payments
WHERE DaysToPayment IS NOT NULL;


-- Average days to payment by payer — identifies slow payers
SELECT
    py.PayerName,
    py.PayerCategory,
    COUNT(p.PaymentKey)                 AS PaymentCount,
    AVG(CAST(p.DaysToPayment AS FLOAT)) AS AvgDaysToPayment,
    MIN(p.DaysToPayment)                AS MinDays,
    MAX(p.DaysToPayment)                AS MaxDays
FROM fact.Payments p
JOIN fact.Claims   c   ON c.ClaimKey  = p.ClaimKey
JOIN dim.Payer     py  ON py.PayerKey = c.PayerKey
WHERE p.DaysToPayment IS NOT NULL
GROUP BY py.PayerKey, py.PayerName, py.PayerCategory
ORDER BY AvgDaysToPayment DESC;


-- Payment speed buckets (A/R aging simulation)
SELECT
    CASE
        WHEN DaysToPayment <=  30 THEN '0-30 days'
        WHEN DaysToPayment <=  60 THEN '31-60 days'
        WHEN DaysToPayment <=  90 THEN '61-90 days'
        WHEN DaysToPayment <= 120 THEN '91-120 days'
        ELSE                           '120+ days'
    END                                 AS AgingBucket,
    COUNT(*)                            AS PaymentCount,
    SUM(PaidAmount)                     AS TotalPaid
FROM fact.Payments
WHERE DaysToPayment IS NOT NULL
GROUP BY
    CASE
        WHEN DaysToPayment <=  30 THEN '0-30 days'
        WHEN DaysToPayment <=  60 THEN '31-60 days'
        WHEN DaysToPayment <=  90 THEN '61-90 days'
        WHEN DaysToPayment <= 120 THEN '91-120 days'
        ELSE                           '120+ days'
    END
ORDER BY MIN(DaysToPayment);


-- =============================================================================
-- 5. Combined KPI summary — single-row dashboard snapshot
-- =============================================================================

SELECT
    total.TotalClaims,
    total.TotalBilled,
    total.TotalPaid,
    total.TotalBilled - total.TotalPaid     AS TotalUnpaid,
    total.TotalPaid
        / NULLIF(total.TotalBilled, 0)      AS CollectionRate,
    denied.DeniedClaims,
    denied.DeniedClaims * 100.0
        / NULLIF(total.TotalClaims, 0)      AS DenialPct,
    pay.AvgDaysToPayment
FROM (
    SELECT
        COUNT(*)                            AS TotalClaims,
        SUM(TotalBilledAmount)              AS TotalBilled,
        SUM(TotalPaidAmount)                AS TotalPaid
    FROM fact.Claims
) total
CROSS JOIN (
    SELECT COUNT(*) AS DeniedClaims
    FROM fact.Claims
    WHERE ClaimStatus = 'Denied'
) denied
CROSS JOIN (
    SELECT AVG(CAST(DaysToPayment AS FLOAT)) AS AvgDaysToPayment
    FROM fact.Payments
    WHERE DaysToPayment IS NOT NULL
) pay;
