import os
import sqlite3
from contextlib import contextmanager

DB_DIR = os.path.join(os.path.dirname(__file__), "data")
DB_PATH = os.path.join(DB_DIR, "compuchem_lite.sqlite")

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS materials (
  material_code TEXT PRIMARY KEY,
  material_name TEXT NOT NULL,
  category TEXT NOT NULL,
  stock_uom TEXT NOT NULL,        -- kg, L, ea
  issue_uom TEXT NOT NULL,        -- kg, g, L, ml, ea
  issue_to_stock_factor REAL NOT NULL DEFAULT 1.0,
  std_wastage_pct REAL NOT NULL DEFAULT 0.0,
  is_critical INTEGER NOT NULL DEFAULT 0,
  active INTEGER NOT NULL DEFAULT 1,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS products (
  product_code TEXT PRIMARY KEY,
  product_name TEXT NOT NULL,
  base_batch_size_l REAL NOT NULL,     -- your standard (e.g., 200 L)
  active INTEGER NOT NULL DEFAULT 1,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS formulas (
  product_code TEXT NOT NULL,
  version TEXT NOT NULL,
  line_no INTEGER NOT NULL,
  material_code TEXT NOT NULL,
  qty_per_base_batch REAL NOT NULL,    -- in STOCK UoM (recommended)
  line_uom TEXT NOT NULL,              -- keep same as stock_uom for simplicity
  is_critical INTEGER NOT NULL DEFAULT 0,
  notes TEXT,
  PRIMARY KEY(product_code, version, line_no),
  FOREIGN KEY(product_code) REFERENCES products(product_code),
  FOREIGN KEY(material_code) REFERENCES materials(material_code)
);

CREATE TABLE IF NOT EXISTS purchases (
  purchase_id INTEGER PRIMARY KEY AUTOINCREMENT,
  purchase_date TEXT NOT NULL,         -- ISO date
  supplier TEXT,
  invoice_no TEXT,
  material_code TEXT NOT NULL,
  qty_in REAL NOT NULL,                -- in STOCK UoM
  unit_cost REAL,                      -- optional now (for costing later)
  notes TEXT,
  FOREIGN KEY(material_code) REFERENCES materials(material_code)
);

CREATE TABLE IF NOT EXISTS batches (
  batch_id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_date TEXT NOT NULL,            -- ISO date
  week_label TEXT,                     -- e.g. "2026-W02"
  product_code TEXT NOT NULL,
  formula_version TEXT NOT NULL,
  batch_size_l REAL NOT NULL,          -- e.g., 200 or 500
  num_batches INTEGER NOT NULL,
  operator TEXT,
  notes TEXT,
  FOREIGN KEY(product_code) REFERENCES products(product_code)
);

-- computed expected consumption per batch entry (frozen snapshot)
CREATE TABLE IF NOT EXISTS batch_consumption (
  consumption_id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id INTEGER NOT NULL,
  material_code TEXT NOT NULL,
  expected_qty_out REAL NOT NULL,      -- in STOCK UoM, negative movement later
  created_at TEXT NOT NULL,
  FOREIGN KEY(batch_id) REFERENCES batches(batch_id) ON DELETE CASCADE,
  FOREIGN KEY(material_code) REFERENCES materials(material_code)
);

-- the stock ledger: ALL movements here (append-only)
CREATE TABLE IF NOT EXISTS inventory_moves (
  move_id INTEGER PRIMARY KEY AUTOINCREMENT,
  move_date TEXT NOT NULL,             -- ISO date
  move_type TEXT NOT NULL,             -- Purchase, BatchConsumption, Adjustment
  ref_type TEXT,                       -- Purchase / Batch / StockTake
  ref_id TEXT,                         -- id as text
  material_code TEXT NOT NULL,
  qty_change REAL NOT NULL,            -- + in, - out (STOCK UoM)
  reason TEXT,
  notes TEXT,
  FOREIGN KEY(material_code) REFERENCES materials(material_code)
);

CREATE TABLE IF NOT EXISTS stocktakes (
    stocktake_id INTEGER PRIMARY KEY AUTOINCREMENT,
    stocktake_date TEXT NOT NULL,
    label TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stocktake_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stocktake_id INTEGER NOT NULL,
    item_name TEXT NOT NULL,
    category TEXT,
    counted_qty REAL NOT NULL,
    uom TEXT,
    linked_material_code TEXT,
    notes TEXT,
    FOREIGN KEY(stocktake_id) REFERENCES stocktakes(stocktake_id) ON DELETE CASCADE

);

CREATE INDEX IF NOT EXISTS idx_moves_material ON inventory_moves(material_code);
CREATE INDEX IF NOT EXISTS idx_moves_date ON inventory_moves(move_date);
"""

def get_conn() -> sqlite3.Connection:
    os.makedirs(DB_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    with get_conn() as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()

@contextmanager
def db_cursor():
    conn = get_conn()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        c = conn.cursor()

        # =========================
        # EXISTING TABLES
        # =========================
        # materials
        # products
        # formulas
        # stock_ledger
        # etc
        # (leave your existing ones here)

        # =========================
        # THINNERS TABLES
        # =========================
        c.execute("""
        CREATE TABLE IF NOT EXISTS thinner_recipes (
            thinner_code TEXT NOT NULL,
            material_code TEXT NOT NULL,
            qty_issue REAL NOT NULL,
            uom TEXT NOT NULL,
            PRIMARY KEY (thinner_code, material_code)
        )
        """)

        c.execute("""
        CREATE TABLE IF NOT EXISTS thinner_sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            sale_date TEXT NOT NULL,
            thinner_code TEXT NOT NULL,
            qty_sold REAL NOT NULL,
            uom TEXT NOT NULL,
            notes TEXT
        )
        """)

        conn.commit()
