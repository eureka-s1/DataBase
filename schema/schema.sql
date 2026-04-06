PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  role TEXT NOT NULL CHECK(role IN ('admin','operator','viewer')),
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_logs (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  action TEXT NOT NULL,
  before_json TEXT,
  after_json TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS customers (
  id INTEGER PRIMARY KEY,
  customer_code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  phone TEXT,
  country TEXT,
  email TEXT,
  default_price_per_m3 NUMERIC NOT NULL DEFAULT 89.71,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS customer_aliases (
  id INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  alias_name TEXT NOT NULL,
  alias_name_norm TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL DEFAULT 'MANUAL' CHECK(source IN ('MANUAL','IMPORT_MAP','AUTO_DETECT')),
  is_primary INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  remark TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS warehouses (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  location TEXT,
  is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS customer_price_rules (
  id INTEGER PRIMARY KEY,
  customer_id INTEGER NOT NULL,
  effective_from TEXT NOT NULL,
  effective_to TEXT,
  price_per_m3 NUMERIC NOT NULL,
  currency TEXT NOT NULL DEFAULT 'CNY',
  remark TEXT,
  UNIQUE(customer_id, effective_from),
  FOREIGN KEY(customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS import_batches (
  id INTEGER PRIMARY KEY,
  batch_no TEXT NOT NULL UNIQUE,
  source_file TEXT NOT NULL,
  sheet_name TEXT,
  import_type TEXT NOT NULL CHECK(import_type IN ('inbound','container','invoice')),
  total_rows INTEGER NOT NULL DEFAULT 0,
  success_rows INTEGER NOT NULL DEFAULT 0,
  failed_rows INTEGER NOT NULL DEFAULT 0,
  error_report_path TEXT,
  created_by INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS inbound_import_rows (
  id INTEGER PRIMARY KEY,
  import_batch_id INTEGER NOT NULL,
  row_no INTEGER NOT NULL,
  inbound_item_id INTEGER,
  is_valid INTEGER NOT NULL DEFAULT 0,
  error_reason TEXT,
  source_sheet TEXT,
  customer_name_raw TEXT,
  item_name_raw TEXT,
  source_row_json TEXT NOT NULL,
  normalized_row_json TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(import_batch_id, row_no),
  FOREIGN KEY(import_batch_id) REFERENCES import_batches(id),
  FOREIGN KEY(inbound_item_id) REFERENCES inbound_items(id)
);

CREATE TABLE IF NOT EXISTS containers (
  id INTEGER PRIMARY KEY,
  container_no TEXT NOT NULL UNIQUE,
  master_customer_id INTEGER,
  container_type TEXT NOT NULL DEFAULT '40HQ',
  capacity_cbm NUMERIC NOT NULL DEFAULT 68.0,
  eta_date TEXT,
  status TEXT NOT NULL CHECK(status IN ('DRAFT','CONFIRMED','REVOKED')),
  price_mode TEXT NOT NULL DEFAULT 'BY_CUSTOMER_RULE' CHECK(price_mode IN ('BY_CUSTOMER_RULE','BY_CONTAINER_DEFAULT')),
  default_price_per_m3 NUMERIC,
  confirmed_at TEXT,
  revoked_at TEXT,
  remark TEXT,
  created_by INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(master_customer_id) REFERENCES customers(id),
  FOREIGN KEY(created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS inbound_items (
  id INTEGER PRIMARY KEY,
  inbound_no TEXT NOT NULL UNIQUE,
  import_batch_id INTEGER,
  customer_id INTEGER NOT NULL,
  warehouse_id INTEGER,
  inbound_date TEXT NOT NULL,
  shop_no TEXT,
  position_or_tel TEXT,
  item_no TEXT,
  item_name_cn TEXT,
  material TEXT,
  carton_count INTEGER,
  qty INTEGER,
  unit_price NUMERIC,
  total_price NUMERIC,
  deposit_hint NUMERIC,
  length_cm NUMERIC,
  width_cm NUMERIC,
  height_cm NUMERIC,
  cbm_calculated NUMERIC NOT NULL DEFAULT 0,
  cbm_override NUMERIC,
  status TEXT NOT NULL DEFAULT 'IN_STOCK' CHECK(status IN ('IN_STOCK','ALLOCATED','SHIPPED')),
  container_id INTEGER,
  remark TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(import_batch_id) REFERENCES import_batches(id),
  FOREIGN KEY(customer_id) REFERENCES customers(id),
  FOREIGN KEY(warehouse_id) REFERENCES warehouses(id),
  FOREIGN KEY(container_id) REFERENCES containers(id)
);

CREATE TABLE IF NOT EXISTS container_items (
  id INTEGER PRIMARY KEY,
  container_id INTEGER NOT NULL,
  inbound_item_id INTEGER NOT NULL,
  cbm_at_load NUMERIC NOT NULL,
  load_order INTEGER,
  remark TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(container_id, inbound_item_id),
  FOREIGN KEY(container_id) REFERENCES containers(id),
  FOREIGN KEY(inbound_item_id) REFERENCES inbound_items(id)
);

CREATE TABLE IF NOT EXISTS payment_transactions (
  id INTEGER PRIMARY KEY,
  payment_no TEXT NOT NULL UNIQUE,
  customer_id INTEGER NOT NULL,
  payment_date TEXT NOT NULL,
  amount NUMERIC NOT NULL,
  currency TEXT NOT NULL DEFAULT 'CNY',
  method TEXT NOT NULL,
  reference_no TEXT,
  remark TEXT,
  created_by INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(customer_id) REFERENCES customers(id),
  FOREIGN KEY(created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS settlement_statements (
  id INTEGER PRIMARY KEY,
  statement_no TEXT NOT NULL UNIQUE,
  container_id INTEGER NOT NULL,
  statement_date TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('DRAFT','POSTED','VOID')),
  currency TEXT NOT NULL DEFAULT 'CNY',
  created_by INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(container_id) REFERENCES containers(id),
  FOREIGN KEY(created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS settlement_lines (
  id INTEGER PRIMARY KEY,
  statement_id INTEGER NOT NULL,
  customer_id INTEGER NOT NULL,
  cbm_total NUMERIC NOT NULL,
  price_per_m3 NUMERIC NOT NULL,
  freight_amount NUMERIC NOT NULL,
  deposit_used NUMERIC NOT NULL DEFAULT 0,
  amount_due NUMERIC NOT NULL DEFAULT 0,
  amount_balance NUMERIC NOT NULL DEFAULT 0,
  remark TEXT,
  UNIQUE(statement_id, customer_id),
  FOREIGN KEY(statement_id) REFERENCES settlement_statements(id),
  FOREIGN KEY(customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS payment_allocations (
  id INTEGER PRIMARY KEY,
  payment_id INTEGER NOT NULL,
  settlement_line_id INTEGER NOT NULL,
  allocated_amount NUMERIC NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(payment_id) REFERENCES payment_transactions(id),
  FOREIGN KEY(settlement_line_id) REFERENCES settlement_lines(id)
);

CREATE TABLE IF NOT EXISTS export_jobs (
  id INTEGER PRIMARY KEY,
  export_type TEXT NOT NULL CHECK(export_type IN ('INBOUND_DAILY','INVENTORY','CONTAINER','STATEMENT','LEDGER')),
  filter_json TEXT,
  file_path TEXT NOT NULL,
  created_by INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY(created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS backup_jobs (
  id INTEGER PRIMARY KEY,
  backup_time TEXT NOT NULL,
  backup_file TEXT NOT NULL,
  size_bytes INTEGER,
  status TEXT NOT NULL CHECK(status IN ('SUCCESS','FAILED')),
  message TEXT
);

CREATE INDEX IF NOT EXISTS idx_inbound_status_date ON inbound_items(status, inbound_date);
CREATE INDEX IF NOT EXISTS idx_inbound_customer ON inbound_items(customer_id);
CREATE INDEX IF NOT EXISTS idx_container_items_container ON container_items(container_id);
CREATE INDEX IF NOT EXISTS idx_customer_aliases_customer ON customer_aliases(customer_id);
CREATE INDEX IF NOT EXISTS idx_import_rows_batch ON inbound_import_rows(import_batch_id);
