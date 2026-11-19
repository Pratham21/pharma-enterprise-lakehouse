-- EXAMPLE ONLY. Adapt catalog, groups, access policies and approvals.
-- Do not apply these grants in a real regulated environment without review.

GRANT USE CATALOG ON CATALOG main TO `pharma_analysts`;
GRANT USE SCHEMA ON SCHEMA main.pharma_gold TO `pharma_analysts`;
GRANT SELECT ON VIEW main.pharma_gold.vw_rx_metrics TO `pharma_analysts`;

-- Stewardship ideas beyond these GRANT statements:
-- 1) Assign explicit owner/steward and approved glossary definitions.
-- 2) Apply governed tags, classifications and access request workflows.
-- 3) Evaluate row filters / column masks for sensitive operational data.
-- 4) Retain audit logs and implement validated retention requirements.
-- 5) Document authorized purposes and regional data-residency constraints.
