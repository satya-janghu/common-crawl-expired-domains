-- Initial per-run state migration.
-- The embedded application migrator applies schemas/run_state_schema.sql for this version
-- and records version 1 in schema_migrations.
SELECT 1;
