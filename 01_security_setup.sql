-- 01_security_setup.sql
-- Run in a Databricks SQL editor or notebook AFTER the pipeline has created the gold tables.
-- Demonstrates PII masking via a view and least-privilege access. Synthetic data only.

-- 1. Masked view for analysts: account_id replaced by a one-way SHA-256 hash,
--    so analysts can still join and count accounts without seeing the real ID.
--    (Production: use a keyed hash / tokenisation with the key held in a secret scope,
--     not a plain hash, because account IDs are low-entropy and a plain hash can be brute-forced.)
CREATE OR REPLACE VIEW workspace.default.gold_account_daily_balance_masked AS
SELECT
  sha2(account_id, 256)               AS account_key,
  txn_date,
  daily_net,
  running_balance
FROM workspace.default.gold_account_daily_balance;

-- 2. Least-privilege access: analysts read the masked view only, never the base table.
--    Replace `analysts` with a real group in your workspace.
-- GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
-- GRANT USE SCHEMA  ON SCHEMA  workspace.default TO `analysts`;
-- GRANT SELECT      ON VIEW    workspace.default.gold_account_daily_balance_masked TO `analysts`;
-- (No SELECT is granted on gold_account_daily_balance or silver_transactions.)

-- 3. Check the masking: real IDs must not appear in the analyst view.
SELECT COUNT(*) AS leaked_ids
FROM workspace.default.gold_account_daily_balance_masked m
JOIN workspace.default.gold_account_daily_balance g
  ON m.account_key = g.account_id;   -- expected: 0
