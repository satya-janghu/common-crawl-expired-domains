-- artifact_schema.sql
-- Optional helper schema/views for artifact manifests.
-- The main artifact_files table lives in run_state_schema.sql.

CREATE VIEW IF NOT EXISTS sealed_artifacts AS
SELECT
  run_id,
  artifact_type,
  path,
  format,
  compression,
  row_count,
  byte_count,
  sha256,
  sealed_at
FROM artifact_files
WHERE status = 'sealed';

CREATE VIEW IF NOT EXISTS export_artifacts AS
SELECT
  run_id,
  path,
  format,
  compression,
  row_count,
  sealed_at
FROM artifact_files
WHERE artifact_type LIKE 'export_%'
  AND status = 'sealed';
