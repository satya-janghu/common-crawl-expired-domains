-- domain_cache_schema.sql
-- Cross-run domain availability cache SQLite schema.
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

CREATE TABLE IF NOT EXISTS cache_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS domains (
  domain TEXT PRIMARY KEY,
  ascii_domain TEXT NOT NULL,
  unicode_domain TEXT,
  tld TEXT NOT NULL,
  psl_version TEXT,
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_status TEXT,
  last_confidence TEXT CHECK (last_confidence IN ('low','medium','high','highest')),
  tld_policy_category TEXT CHECK (tld_policy_category IN ('open_retail','restricted','reserved_infrastructure','unsupported_unknown')),
  classification_reason TEXT,
  last_checked_at TEXT,
  next_check_at TEXT,
  score INTEGER NOT NULL DEFAULT 0,
  watchlisted INTEGER NOT NULL DEFAULT 0 CHECK (watchlisted IN (0,1)),
  notes TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_domains_status ON domains(last_status);
CREATE INDEX IF NOT EXISTS idx_domains_next_check ON domains(next_check_at);
CREATE INDEX IF NOT EXISTS idx_domains_tld ON domains(tld);

CREATE TABLE IF NOT EXISTS availability_checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  domain TEXT NOT NULL REFERENCES domains(domain) ON DELETE CASCADE,
  method TEXT NOT NULL CHECK (method IN ('dns','rdap','registrar','classifier')),
  provider TEXT,
  status TEXT NOT NULL,
  confidence TEXT CHECK (confidence IN ('low','medium','high','highest')),
  raw_response_sha256 TEXT,
  summary_json TEXT,
  checked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TEXT,
  latency_ms INTEGER,
  error_code TEXT,
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_availability_domain_checked ON availability_checks(domain, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_availability_expires ON availability_checks(expires_at);

CREATE TABLE IF NOT EXISTS dns_cache (
  domain TEXT NOT NULL REFERENCES domains(domain) ON DELETE CASCADE,
  record_type TEXT NOT NULL,
  status TEXT NOT NULL,
  answers_json TEXT,
  ttl_seconds INTEGER,
  checked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TEXT NOT NULL,
  PRIMARY KEY (domain, record_type)
);

CREATE INDEX IF NOT EXISTS idx_dns_expires ON dns_cache(expires_at);

CREATE TABLE IF NOT EXISTS rdap_cache (
  domain TEXT PRIMARY KEY REFERENCES domains(domain) ON DELETE CASCADE,
  rdap_endpoint TEXT,
  http_status INTEGER,
  rdap_status TEXT,
  event_summary_json TEXT,
  response_sha256 TEXT,
  checked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TEXT NOT NULL,
  error_code TEXT,
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_rdap_expires ON rdap_cache(expires_at);

CREATE TABLE IF NOT EXISTS registrar_cache (
  domain TEXT NOT NULL REFERENCES domains(domain) ON DELETE CASCADE,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  available INTEGER CHECK (available IN (0,1)),
  premium INTEGER CHECK (premium IN (0,1)),
  price_cents INTEGER,
  currency TEXT,
  response_sha256 TEXT,
  summary_json TEXT,
  checked_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  expires_at TEXT NOT NULL,
  error_code TEXT,
  error_message TEXT,
  PRIMARY KEY (domain, provider)
);

CREATE INDEX IF NOT EXISTS idx_registrar_expires ON registrar_cache(expires_at);

CREATE TABLE IF NOT EXISTS domain_sources (
  domain TEXT NOT NULL REFERENCES domains(domain) ON DELETE CASCADE,
  source_domain TEXT NOT NULL,
  mode TEXT NOT NULL CHECK (mode IN ('web_graph','wat','warc')),
  crawl_or_graph_id TEXT NOT NULL,
  seen_count INTEGER NOT NULL DEFAULT 1,
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (domain, source_domain, mode, crawl_or_graph_id)
);

CREATE TABLE IF NOT EXISTS tld_support (
  tld TEXT PRIMARY KEY,
  policy_category TEXT NOT NULL DEFAULT 'unsupported_unknown' CHECK (policy_category IN ('open_retail','restricted','reserved_infrastructure','unsupported_unknown')),
  rdap_supported INTEGER NOT NULL DEFAULT 0 CHECK (rdap_supported IN (0,1)),
  registrar_supported INTEGER NOT NULL DEFAULT 0 CHECK (registrar_supported IN (0,1)),
  default_provider TEXT,
  policy_json TEXT,
  policy_refreshed_at TEXT,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ignore_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_type TEXT NOT NULL CHECK (rule_type IN ('domain','suffix','tld','regex')),
  pattern TEXT NOT NULL,
  reason TEXT,
  enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (rule_type, pattern)
);

CREATE TABLE IF NOT EXISTS watchlist (
  domain TEXT PRIMARY KEY REFERENCES domains(domain) ON DELETE CASCADE,
  reason TEXT,
  next_check_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
