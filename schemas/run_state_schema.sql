-- run_state_schema.sql
-- Per-run crash-recoverable SQLite schema.
-- Apply PRAGMAs in application code on every connection:
-- PRAGMA journal_mode=WAL;
-- PRAGMA synchronous=NORMAL;
-- PRAGMA foreign_keys=ON;
-- PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN (
    'created','running','pause_requested','paused','resume_requested',
    'cancelling','cancelled','completed','failed','recoverable'
  )),
  app_version TEXT,
  config_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  started_at TEXT,
  completed_at TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS run_source_domains (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  source_domain TEXT NOT NULL,
  original_input TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, source_domain)
);

CREATE TABLE IF NOT EXISTS run_crawl_selections (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  selection_type TEXT NOT NULL CHECK (selection_type IN ('crawl_id','month','graph_release')),
  selection_value TEXT NOT NULL,
  resolved_id TEXT NOT NULL,
  metadata_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, selection_type, resolved_id)
);

CREATE TABLE IF NOT EXISTS extraction_modes (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  mode TEXT NOT NULL CHECK (mode IN ('web_graph','wat','warc')),
  enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),
  PRIMARY KEY (run_id, mode)
);

CREATE TABLE IF NOT EXISTS cdx_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  source_domain TEXT NOT NULL,
  crawl_id TEXT NOT NULL,
  url TEXT NOT NULL,
  urlkey TEXT,
  timestamp TEXT,
  mime TEXT,
  status INTEGER,
  digest TEXT,
  length INTEGER,
  offset INTEGER,
  filename TEXT,
  raw_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (run_id, crawl_id, urlkey, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_cdx_run_source ON cdx_records(run_id, source_domain);
CREATE INDEX IF NOT EXISTS idx_cdx_digest ON cdx_records(run_id, digest);
CREATE INDEX IF NOT EXISTS idx_cdx_warc ON cdx_records(run_id, filename, offset, length);

CREATE TABLE IF NOT EXISTS cdx_discovery_pages (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  source_domain TEXT NOT NULL,
  crawl_id TEXT NOT NULL,
  pattern TEXT NOT NULL,
  page INTEGER NOT NULL,
  page_size INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','running','succeeded','failed_retryable','failed_final')) DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  record_count INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, source_domain, crawl_id, pattern, page)
);

CREATE INDEX IF NOT EXISTS idx_cdx_pages_state ON cdx_discovery_pages(run_id, state);

CREATE TABLE IF NOT EXISTS warc_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  crawl_id TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  cdx_record_id INTEGER REFERENCES cdx_records(id) ON DELETE CASCADE,
  filename TEXT NOT NULL,
  offset INTEGER NOT NULL,
  length INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','leased','running','succeeded','failed_retryable','failed_final','cancelled')) DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8,
  lease_owner TEXT,
  leased_until TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (run_id, crawl_id, filename, offset, length)
);

CREATE INDEX IF NOT EXISTS idx_warc_tasks_state ON warc_tasks(run_id, state, leased_until);

CREATE TABLE IF NOT EXISTS wat_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  crawl_id TEXT NOT NULL,
  source_domain_set_hash TEXT NOT NULL,
  wat_filename TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','leased','running','succeeded','failed_retryable','failed_final','cancelled')) DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8,
  lease_owner TEXT,
  leased_until TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (run_id, crawl_id, wat_filename, source_domain_set_hash)
);

CREATE INDEX IF NOT EXISTS idx_wat_tasks_state ON wat_tasks(run_id, state, leased_until);

CREATE TABLE IF NOT EXISTS graph_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  graph_release_id TEXT NOT NULL,
  task_type TEXT NOT NULL CHECK (task_type IN ('vertex_source_scan','edge_scan','vertex_target_resolve')),
  file_path TEXT NOT NULL,
  source_domain_set_hash TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending','leased','running','succeeded','failed_retryable','failed_final','cancelled')) DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8,
  lease_owner TEXT,
  leased_until TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (run_id, graph_release_id, task_type, file_path, source_domain_set_hash)
);

CREATE INDEX IF NOT EXISTS idx_graph_tasks_state ON graph_tasks(run_id, state, leased_until);

CREATE TABLE IF NOT EXISTS candidate_domains (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  domain TEXT NOT NULL,
  tld TEXT,
  first_seen_mode TEXT CHECK (first_seen_mode IN ('web_graph','wat','warc')),
  seen_in_graph INTEGER NOT NULL DEFAULT 0 CHECK (seen_in_graph IN (0,1)),
  seen_in_wat INTEGER NOT NULL DEFAULT 0 CHECK (seen_in_wat IN (0,1)),
  seen_in_warc INTEGER NOT NULL DEFAULT 0 CHECK (seen_in_warc IN (0,1)),
  source_page_count INTEGER NOT NULL DEFAULT 0,
  graph_edge_count INTEGER NOT NULL DEFAULT 0,
  outbound_url_count INTEGER NOT NULL DEFAULT 0,
  availability_status TEXT,
  confidence TEXT CHECK (confidence IN ('low','medium','high','highest')),
  tld_policy_category TEXT CHECK (tld_policy_category IN ('open_retail','restricted','reserved_infrastructure','unsupported_unknown')),
  classification_reason TEXT,
  registrar_provider TEXT,
  premium INTEGER NOT NULL DEFAULT 0 CHECK (premium IN (0,1)),
  price_cents INTEGER,
  currency TEXT,
  score INTEGER NOT NULL DEFAULT 0,
  cache_hit INTEGER NOT NULL DEFAULT 0 CHECK (cache_hit IN (0,1)),
  watchlisted INTEGER NOT NULL DEFAULT 0 CHECK (watchlisted IN (0,1)),
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_checked_at TEXT,
  notes TEXT,
  PRIMARY KEY (run_id, domain)
);

CREATE INDEX IF NOT EXISTS idx_candidate_status ON candidate_domains(run_id, availability_status);
CREATE INDEX IF NOT EXISTS idx_candidate_score ON candidate_domains(run_id, score DESC);

CREATE TABLE IF NOT EXISTS candidate_sources (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  domain TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('web_graph','wat','warc')),
  crawl_or_graph_id TEXT NOT NULL,
  source_url_sample TEXT,
  anchor_sample TEXT,
  seen_count INTEGER NOT NULL DEFAULT 1,
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, domain, source_domain, mode, crawl_or_graph_id),
  FOREIGN KEY (run_id, domain) REFERENCES candidate_domains(run_id, domain) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS candidate_source_pages (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  domain TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('wat','warc')),
  crawl_id TEXT NOT NULL,
  source_url TEXT NOT NULL,
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, domain, source_domain, mode, crawl_id, source_url),
  FOREIGN KEY (run_id, domain) REFERENCES candidate_domains(run_id, domain) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS availability_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  domain TEXT NOT NULL,
  method TEXT NOT NULL CHECK (method IN ('dns','rdap','registrar','classifier')),
  provider TEXT,
  state TEXT NOT NULL CHECK (state IN ('pending','leased','running','succeeded','failed_retryable','failed_final','cancelled')) DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 8,
  next_attempt_at TEXT,
  lease_owner TEXT,
  leased_until TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (run_id, domain, method, provider)
);

CREATE INDEX IF NOT EXISTS idx_availability_tasks_state ON availability_tasks(run_id, state, next_attempt_at, leased_until);

CREATE TABLE IF NOT EXISTS progress_counters (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  stage TEXT NOT NULL CHECK (stage IN ('discovery','extraction','domain_checks','export')),
  completed_units INTEGER NOT NULL DEFAULT 0,
  total_known_units INTEGER NOT NULL DEFAULT 0,
  projected_remaining_units INTEGER NOT NULL DEFAULT 0,
  failed_units INTEGER NOT NULL DEFAULT 0,
  recent_rate REAL NOT NULL DEFAULT 0,
  eta_seconds INTEGER,
  confidence TEXT CHECK (confidence IN ('low','medium','high')) DEFAULT 'low',
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, stage)
);

CREATE TABLE IF NOT EXISTS artifact_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  artifact_type TEXT NOT NULL,
  path TEXT NOT NULL,
  format TEXT NOT NULL,
  compression TEXT,
  row_count INTEGER,
  byte_count INTEGER,
  sha256 TEXT,
  status TEXT NOT NULL CHECK (status IN ('writing','sealed','failed','ignored')),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sealed_at TEXT,
  UNIQUE (run_id, path)
);

CREATE TABLE IF NOT EXISTS observation_batches (
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  batch_key TEXT NOT NULL,
  artifact_path TEXT,
  artifact_type TEXT,
  row_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (run_id, batch_key)
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  severity TEXT NOT NULL DEFAULT 'info' CHECK (severity IN ('debug','info','warn','error')),
  component TEXT,
  message TEXT,
  payload_json TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_events_run_created ON events(run_id, created_at, id);

CREATE TABLE IF NOT EXISTS errors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  component TEXT NOT NULL,
  task_table TEXT,
  task_id INTEGER,
  error_code TEXT,
  error_message TEXT NOT NULL,
  retryable INTEGER NOT NULL DEFAULT 0 CHECK (retryable IN (0,1)),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
