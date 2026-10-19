-- Reverse of 0001_init: drop everything in reverse dependency order.

DROP TABLE IF EXISTS list_entries;
DROP TABLE IF EXISTS replay_jobs;
DROP TABLE IF EXISTS audit_log;
DROP TABLE IF EXISTS labels;
DROP TABLE IF EXISTS cases;
DROP TABLE IF EXISTS decisions;
DROP TABLE IF EXISTS rule_sets;
