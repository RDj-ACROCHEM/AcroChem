# logic.py
# Core business + database engine for CompuChem
# NO Streamlit code in this file. EVER.

import sqlite3
import pandas as pd
from datetime import datetime
from pathlib import Path

# =============================
# DATABASE
# =============================

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "compuchem_lite.sqlite"

DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_conn():
    return sqlite3.connect(DB_PATH)


# =============================
# INITIALISE DATABASE
# =============================

def init_db():
    with get_conn() as conn:
        c = conn.cursor()

        # ---- MATERIALS (RM MASTER)
        c.execute("""
        CREATE TABLE IF NOT EXISTS materials (
            material_code TEXT PRIMARY KEY,
            material_name TEXT NOT NULL,
            category TEXT NOT NULL,
            stock_uom TEXT NOT NULL,
            issue_uom TEXT NOT NULL,
            issue_to_stock_factor REAL NOT NULL DEFAULT 1.0,
            std_wastage_pct REAL NOT NULL DEFAULT 0.0,
            is_critical INTEGER NOT NULL DEFAULT 0,
            active INTEGER NOT NULL DEFAULT 1,
            notes TEXT
        )
        """)

        # ---- PRODUCTS
        c.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_code TEXT PRIMARY KEY,
            product_name TEXT NOT NULL,
            category TEXT,
            active INTEGER NOT NULL DEFAULT 1
        )
        """)

        # ---- FORMULAS
        c.execute("""
        CREATE TABLE IF NOT EXISTS formulas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_code TEXT NOT NULL,
            material_code TEXT NOT NULL,
            qty REAL NOT NULL,
            uom TEXT NOT NULL,
            FOREIGN KEY(product_code) REFERENCES products(product_code),
            FOREIGN KEY(material_code) REFERENCES materials(material_code)
        )
        """)

        # ---- LEDGER (ALL MOVEMENTS)
        c.execute("""
        CREATE TABLE IF NOT EXISTS stock_ledger (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            ref_type TEXT NOT NULL,
            ref_no TEXT,
            material_code TEXT NOT NULL,
            qty REAL NOT NULL,
            uom TEXT NOT NULL,
            qty_stock REAL NOT NULL,
            cost_per_stock REAL NOT NULL,
            total_cost REAL NOT NULL,
            note TEXT,
            FOREIGN KEY(material_code) REFERENCES materials(material_code)
        )
        """)

        # ---- STOCKTAKE SNAPSHOTS
        c.execute("""
        CREATE TABLE IF NOT EXISTS stocktake (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            material_code TEXT NOT NULL,
            counted_qty_stock REAL NOT NULL,
            system_qty_stock REAL NOT NULL,
            variance REAL NOT NULL,
            note TEXT
        )
        """)

        conn.commit()


# =============================
# MATERIALS
# =============================

def upsert_material(data: dict):
    with get_conn() as conn:
        conn.execute("""
        INSERT INTO materials (
            material_code, material_name, category,
            stock_uom, issue_uom, issue_to_stock_factor,
            std_wastage_pct, is_critical, active, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(material_code) DO UPDATE SET
            material_name=excluded.material_name,
            category=excluded.category,
            stock_uom=excluded.stock_uom,
            issue_uom=excluded.issue_uom,
            issue_to_stock_factor=excluded.issue_to_stock_factor,
            std_wastage_pct=excluded.std_wastage_pct,
            is_critical=excluded.is_critical,
            active=excluded.active,
            notes=excluded.notes
        """, (
            data["material_code"],
            data["material_name"],
            data["category"],
            data["stock_uom"],
            data["issue_uom"],
            data["issue_to_stock_factor"],
            data["std_wastage_pct"],
            data["is_critical"],
            data["active"],
            data.get("notes", "")
        ))


def get_materials(active_only=True):
    q = "SELECT * FROM materials"
    if active_only:
        q += " WHERE active=1"
    return pd.read_sql(q, get_conn())


def delete_material(material_code: str):
    """
    HARD DELETE – allowed only if no ledger or formula references exist
    """
    with get_conn() as conn:
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM stock_ledger WHERE material_code=?", (material_code,))
        if c.fetchone()[0] > 0:
            raise ValueError("Material has stock movements and cannot be deleted")

        c.execute("SELECT COUNT(*) FROM formulas WHERE material_code=?", (material_code,))
        if c.fetchone()[0] > 0:
            raise ValueError("Material used in formulas and cannot be deleted")

        c.execute("DELETE FROM materials WHERE material_code=?", (material_code,))
        conn.commit()


# =============================
# STOCK ENGINE
# =============================

def _current_stock(material_code):
    q = """
    SELECT
        COALESCE(SUM(qty_stock), 0),
        COALESCE(SUM(total_cost), 0)
    FROM stock_ledger
    WHERE material_code=?
    """
    qty, cost = get_conn().execute(q, (material_code,)).fetchone()
    avg_cost = cost / qty if qty != 0 else 0
    return qty, avg_cost


def post_stock(
    material_code: str,
    qty: float,
    uom: str,
    cost_per_stock: float,
    ref_type: str,
    ref_no: str = None,
    note: str = ""
):
    date = datetime.now().isoformat()

    qty_stock = qty  # already converted BEFORE calling this
    total_cost = qty_stock * cost_per_stock

    with get_conn() as conn:
        conn.execute("""
        INSERT INTO stock_ledger (
            date, ref_type, ref_no,
            material_code, qty, uom,
            qty_stock, cost_per_stock, total_cost, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            date, ref_type, ref_no,
            material_code, qty, uom,
            qty_stock, cost_per_stock, total_cost, note
        ))


def receive_purchase(material_code, qty_stock, total_cost, ref_no=None):
    cost_per_stock = total_cost / qty_stock if qty_stock else 0
    post_stock(
        material_code=material_code,
        qty=qty_stock,
        uom="STOCK",
        cost_per_stock=cost_per_stock,
        ref_type="PURCHASE",
        ref_no=ref_no
    )


def issue_stock(material_code, qty_stock, ref_type, ref_no=None):
    current_qty, avg_cost = _current_stock(material_code)

    if qty_stock > current_qty:
        raise ValueError("Insufficient stock")

    post_stock(
        material_code=material_code,
        qty=-qty_stock,
        uom="STOCK",
        cost_per_stock=avg_cost,
        ref_type=ref_type,
        ref_no=ref_no
    )


def stock_on_hand():
    q = """
    SELECT
        material_code,
        SUM(qty_stock) AS qty_stock,
        SUM(total_cost) AS total_cost,
        CASE WHEN SUM(qty_stock) != 0
             THEN SUM(total_cost) / SUM(qty_stock)
             ELSE 0 END AS avg_cost
    FROM stock_ledger
    GROUP BY material_code
    """
    return pd.read_sql(q, get_conn())

def init_thinner_tables():
    conn = get_connection()

    conn.execute("""
    CREATE TABLE IF NOT EXISTS thinner_recipes (
        thinner_code TEXT NOT NULL,
        material_code TEXT NOT NULL,
        qty REAL NOT NULL,
        uom TEXT NOT NULL,
        PRIMARY KEY (thinner_code, material_code)
    )
    """)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS thinner_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sale_date TEXT NOT NULL,
        thinner_code TEXT NOT NULL,
        qty_liters REAL NOT NULL
    )
    """)

    conn.commit()
    conn.close()


# =============================
# STOCKTAKE
# =============================

def post_stocktake(material_code, counted_qty_stock, note=""):
    system_qty, avg_cost = _current_stock(material_code)
    variance = counted_qty_stock - system_qty

    with get_conn() as conn:
        conn.execute("""
        INSERT INTO stocktake (
            date, material_code,
            counted_qty_stock, system_qty_stock,
            variance, note
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().isoformat(),
            material_code,
            counted_qty_stock,
            system_qty,
            variance,
            note
        ))

    # Post variance to ledger
    if variance != 0:
        post_stock(
            material_code=material_code,
            qty=variance,
            uom="STOCK",
            cost_per_stock=avg_cost,
            ref_type="STOCKTAKE",
            note="Stocktake adjustment"
        )


# =============================
# PRODUCTS & FORMULAS
# =============================

def upsert_product(code, name, category="", active=1):
    with get_conn() as conn:
        conn.execute("""
        INSERT INTO products (product_code, product_name, category, active)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(product_code) DO UPDATE SET
            product_name=excluded.product_name,
            category=excluded.category,
            active=excluded.active
        """, (code, name, category, active))


def set_formula(product_code, rows: list):
    with get_conn() as conn:
        conn.execute("DELETE FROM formulas WHERE product_code=?", (product_code,))
        for r in rows:
            conn.execute("""
            INSERT INTO formulas (product_code, material_code, qty, uom)
            VALUES (?, ?, ?, ?)
            """, (
                product_code,
                r["material_code"],
                r["qty"],
                r["uom"]
            ))


def calculate_batch_cost(product_code, batch_size):
    f = pd.read_sql(
        "SELECT * FROM formulas WHERE product_code=?",
        get_conn(),
        params=(product_code,)
    )

    total_cost = 0

    for _, row in f.iterrows():
        qty_needed = row["qty"] * batch_size
        _, avg_cost = _current_stock(row["material_code"])
        total_cost += qty_needed * avg_cost

    return total_cost

import sqlite3
import pandas as pd

DB_PATH = "data/compuchem_lite.sqlite"

def get_thinner_recipes(thinner_code):
    conn = get_connection()
    df = pd.read_sql("""
        SELECT material_code, qty, uom
        FROM thinner_recipes
        WHERE thinner_code = ?
        ORDER BY material_code
    """, conn, params=(thinner_code,))
    conn.close()
    return df


def upsert_thinner_recipe(thinner_code, lines):
    conn = get_connection()

    conn.execute(
        "DELETE FROM thinner_recipes WHERE thinner_code = ?",
        (thinner_code,)
    )

    for line in lines:
        conn.execute("""
            INSERT INTO thinner_recipes
            (thinner_code, material_code, qty, uom)
            VALUES (?, ?, ?, ?)
        """, (
            thinner_code,
            line["material_code"],
            line["qty"],
            line["uom"]
        ))

    conn.commit()
    conn.close()


# =====================================================
# GET ALL MATERIALS
# =====================================================
def get_all_materials_df():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("""
        SELECT
            material_code,
            material_name,
            category,
            stock_uom,
            issue_uom,
            issue_to_stock_factor,
            std_wastage_pct,
            is_critical,
            active,
            notes
        FROM materials
        ORDER BY material_name
    """, conn)
    conn.close()
    return df


# =====================================================
# INSERT OR UPDATE MATERIAL
# =====================================================
def upsert_material(data):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO materials (
            material_code,
            material_name,
            category,
            stock_uom,
            issue_uom,
            issue_to_stock_factor,
            std_wastage_pct,
            is_critical,
            active,
            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(material_code)
        DO UPDATE SET
            material_name = excluded.material_name,
            category = excluded.category,
            stock_uom = excluded.stock_uom,
            issue_uom = excluded.issue_uom,
            issue_to_stock_factor = excluded.issue_to_stock_factor,
            std_wastage_pct = excluded.std_wastage_pct,
            is_critical = excluded.is_critical,
            active = excluded.active,
            notes = excluded.notes
    """, (
        data["material_code"],
        data["material_name"],
        data["category"],
        data["stock_uom"],
        data["issue_uom"],
        data["issue_to_stock_factor"],
        data["std_wastage_pct"],
        data["is_critical"],
        data["active"],
        data.get("notes", "")
    ))

    conn.commit()
    conn.close()


# =====================================================
# DELETE MATERIAL
# =====================================================
def delete_material(material_code):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        "DELETE FROM materials WHERE material_code = ?",
        (material_code,)
    )

    conn.commit()
    conn.close()


# ============================
# PRODUCTS
# ============================

def get_all_products_df():
    return pd.read_sql(
        "SELECT product_code, product_name, category, active FROM products ORDER BY product_name",
        get_conn()
    )


def save_product(data):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT OR REPLACE INTO products
            (product_code, product_name, category, active)
            VALUES (?, ?, ?, ?)
        """, (
            data["product_code"],
            data["product_name"],
            data.get("category", "Unassigned"),
            data.get("active", 1)
        ))
        conn.commit()


def delete_product(product_code):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM products WHERE product_code = ?", (product_code,))
        conn.commit()
# ============================
# FORMULAS (BOM)
# ============================

def get_formula_lines(product_code):
    return pd.read_sql("""
        SELECT f.material_code,
               m.material_name,
               f.qty,
               f.uom
        FROM formulas f
        JOIN materials m ON f.material_code = m.material_code
        WHERE f.product_code = ?
        ORDER BY m.material_name
    """, get_conn(), params=(product_code,))


def add_formula_line(product_code, material_code, qty, uom):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO formulas (product_code, material_code, qty, uom)
            VALUES (?, ?, ?, ?)
        """, (product_code, material_code, qty, uom))
        conn.commit()


def delete_formula_line(product_code, material_code):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("""
            DELETE FROM formulas
            WHERE product_code = ? AND material_code = ?
        """, (product_code, material_code))
        conn.commit()
# ===============================
# FORMULAS (BOM) LOGIC
# ===============================

def add_formula_line(product_code: str, material_code: str, qty: float, uom: str):
    with get_conn() as conn:
        c = conn.cursor()

        # Prevent duplicates (same product + material)
        c.execute("""
            SELECT COUNT(*) FROM formulas
            WHERE product_code=? AND material_code=?
        """, (product_code, material_code))

        if c.fetchone()[0] > 0:
            raise ValueError("Material already exists in this formula")

        c.execute("""
            INSERT INTO formulas (
                product_code,
                material_code,
                qty,
                uom
            ) VALUES (?, ?, ?, ?)
        """, (product_code, material_code, qty, uom))

        conn.commit()


def get_formula(product_code: str):
    q = """
        SELECT
            f.id,
            f.material_code,
            m.material_name,
            f.qty,
            f.uom
        FROM formulas f
        JOIN materials m
            ON f.material_code = m.material_code
        WHERE f.product_code = ?
        ORDER BY m.material_name
    """
    return pd.read_sql(q, get_conn(), params=(product_code,))


def delete_formula_line(formula_id: int):
    with get_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM formulas WHERE id=?", (formula_id,))
        conn.commit()
# ===============================
# SAFE WRAPPER FOR PRODUCTS
# ===============================

def get_products(active_only=True):
    """
    Compatibility wrapper.
    Does NOT assume active_only support.
    """
    df = get_all_products_df()

    if active_only and "active" in df.columns:
        df = df[df["active"] == 1]

    return df

from db import get_conn
import pandas as pd

# -----------------------------
# THINNER RECIPES
# -----------------------------

def get_thinner_recipes():
    with get_conn() as conn:
        return pd.read_sql("""
            SELECT thinner_code, material_code, ratio
            FROM thinner_recipes
        """, conn)

def upsert_thinner_recipe(thinner_code, lines):
    with get_conn() as conn:
        conn.execute("DELETE FROM thinner_recipes WHERE thinner_code=?", (thinner_code,))
        for line in lines:
            conn.execute("""
                INSERT INTO thinner_recipes (thinner_code, material_code, ratio)
                VALUES (?, ?, ?)
            """, (thinner_code, line["material_code"], line["ratio"]))
        conn.commit()

# -----------------------------
# THINNER SALES
# -----------------------------

def record_thinner_sale(sale_date, thinner_code, qty_liters):
    conn = get_connection()

    conn.execute("""
        INSERT INTO thinner_sales
        (sale_date, thinner_code, qty_liters)
        VALUES (?, ?, ?)
    """, (sale_date, thinner_code, qty_liters))

    conn.commit()
    conn.close()


def calculate_thinner_breakdown(thinner_code, qty_liters):
    recipe = get_thinner_recipes(thinner_code)

    if recipe.empty:
        return pd.DataFrame()

    recipe["component_qty"] = recipe["qty"] * qty_liters
    return recipe
import sqlite3
import pandas as pd
from datetime import datetime

# ----------------------------
# THINNERS: schema + helpers
# ----------------------------

def ensure_thinners_schema():
    """Create thinner tables if they don't exist (safe to call every run)."""
    with get_conn() as conn:
        c = conn.cursor()

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


def get_materials_lookup(active_only=True):
    q = "SELECT material_code, material_name, stock_uom, issue_uom, issue_to_stock_factor FROM materials"
    if active_only:
        q += " WHERE active=1"
    df = pd.read_sql(q, get_conn())
    # Avoid NaN showing in dropdowns
    df["material_name"] = df["material_name"].fillna(df["material_code"])
    return df


def upsert_thinner_recipe(thinner_code: str, lines: list[dict]):
    """
    lines: [{"material_code": "...", "qty_issue": 1.23, "uom": "L"}, ...]
    Replaces existing recipe for that thinner_code.
    """
    ensure_thinners_schema()

    with get_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM thinner_recipes WHERE thinner_code=?", (thinner_code,))
        for ln in lines:
            c.execute(
                "INSERT INTO thinner_recipes (thinner_code, material_code, qty_issue, uom) VALUES (?,?,?,?)",
                (thinner_code, ln["material_code"], float(ln["qty_issue"]), ln["uom"])
            )
        conn.commit()


def get_thinner_recipe_df(thinner_code: str):
    ensure_thinners_schema()
    df = pd.read_sql("""
        SELECT r.thinner_code, r.material_code, m.material_name, r.qty_issue, r.uom
        FROM thinner_recipes r
        LEFT JOIN materials m ON m.material_code = r.material_code
        WHERE r.thinner_code = ?
        ORDER BY r.material_code
    """, get_conn(), params=(thinner_code,))
    df["material_name"] = df["material_name"].fillna(df["material_code"])
    return df


def delete_thinner_recipe_line(thinner_code: str, material_code: str):
    ensure_thinners_schema()
    with get_conn() as conn:
        conn.execute(
            "DELETE FROM thinner_recipes WHERE thinner_code=? AND material_code=?",
            (thinner_code, material_code)
        )
        conn.commit()


def record_thinner_sale_and_deduct_stock(sale_date, thinner_code, qty_sold, uom="L", notes=""):
    """
    Records a sale and deducts underlying raw materials from stock_ledger using the recipe.
    Assumption: recipe qty_issue is per 1 unit of thinner sold (usually 1L basis).
    """
    ensure_thinners_schema()

    # Pull recipe
    recipe = pd.read_sql("""
        SELECT r.material_code, r.qty_issue, r.uom,
               m.issue_to_stock_factor, m.stock_uom
        FROM thinner_recipes r
        LEFT JOIN materials m ON m.material_code = r.material_code
        WHERE r.thinner_code = ?
    """, get_conn(), params=(thinner_code,))

    if recipe.empty:
        raise ValueError(f"No recipe saved for thinner '{thinner_code}'. Save a recipe first.")

    qty_sold = float(qty_sold)

    with get_conn() as conn:
        c = conn.cursor()

        # Save sale record
        c.execute("""
            INSERT INTO thinner_sales (sale_date, thinner_code, qty_sold, uom, notes)
            VALUES (?,?,?,?,?)
        """, (str(sale_date), thinner_code, qty_sold, uom, notes))

        # Deduct raw materials via stock_ledger
        # We try to compute cost using current average cost if your ledger stores total_cost.
        for _, r in recipe.iterrows():
            material_code = r["material_code"]
            per_unit_issue = float(r["qty_issue"])      # e.g. 0.67 L xylene per 1L thinner
            factor = float(r["issue_to_stock_factor"] or 1.0)

            qty_issue_total = per_unit_issue * qty_sold
            qty_stock_total = qty_issue_total * factor  # convert issue uom -> stock uom

            # Get current avg cost from your stock engine if possible
            cur = conn.execute("""
                SELECT
                    COALESCE(SUM(qty_stock), 0) AS qty_stock,
                    COALESCE(SUM(total_cost), 0) AS total_cost
                FROM stock_ledger
                WHERE material_code=?
            """, (material_code,))
            qty_on_hand, total_cost_on_hand = cur.fetchone()
            qty_on_hand = float(qty_on_hand or 0)
            total_cost_on_hand = float(total_cost_on_hand or 0)

            unit_cost = (total_cost_on_hand / qty_on_hand) if qty_on_hand != 0 else 0.0
            cost_to_remove = unit_cost * qty_stock_total

            # IMPORTANT: qty_stock is negative for issues
            c.execute("""
                INSERT INTO stock_ledger (date, material_code, movement_type, ref, qty_stock, total_cost, notes)
                VALUES (?,?,?,?,?,?,?)
            """, (
                str(sale_date),
                material_code,
                "ISSUE",
                f"THINNER SALE {thinner_code}",
                -qty_stock_total,
                -cost_to_remove,
                f"Auto-deduct from thinner sale: {thinner_code} ({qty_sold} {uom})"
            ))

        conn.commit()


def get_thinner_sales_df(limit=200):
    ensure_thinners_schema()
    df = pd.read_sql("""
        SELECT sale_id, sale_date, thinner_code, qty_sold, uom, notes
        FROM thinner_sales
        ORDER BY sale_id DESC
        LIMIT ?
    """, get_conn(), params=(int(limit),))
    return df
# =========================================================
# STOCK ON HAND (VIEW + MANUAL ADJUSTMENTS)
# =========================================================

def get_stock_on_hand():
    conn = get_conn()

    # Raw materials stock
    rm = pd.read_sql("""
        SELECT
            l.item_code AS code,
            m.material_name AS name,
            m.uom AS uom,
            SUM(l.qty_delta) AS qty,
            CASE 
                WHEN SUM(l.qty_delta) = 0 THEN 0
                ELSE SUM(l.cost_delta) / SUM(l.qty_delta)
            END AS avg_cost,
            'RM' AS item_type
        FROM stock_ledger l
        JOIN raw_materials m
            ON m.material_code = l.item_code
        WHERE l.item_type = 'RM'
        GROUP BY l.item_code, m.material_name, m.uom
        HAVING ABS(SUM(l.qty_delta)) > 0.00001
    """, conn)

    # Products stock
    prod = pd.read_sql("""
        SELECT
            l.item_code AS code,
            p.product_name AS name,
            p.uom AS uom,
            SUM(l.qty_delta) AS qty,
            CASE 
                WHEN SUM(l.qty_delta) = 0 THEN 0
                ELSE SUM(l.cost_delta) / SUM(l.qty_delta)
            END AS avg_cost,
            'PRODUCT' AS item_type
        FROM stock_ledger l
        JOIN products p
            ON p.product_code = l.item_code
        WHERE l.item_type = 'PRODUCT'
        GROUP BY l.item_code, p.product_name, p.uom
        HAVING ABS(SUM(l.qty_delta)) > 0.00001
    """, conn)

    df = pd.concat([rm, prod], ignore_index=True)

    return df.sort_values(["item_type", "name"])
# ===========================
# STOCK ON HAND (YOUR SCHEMA)
# stock_ledger columns:
# id, date, ref_type, ref_no, material_code, qty, uom, qty_stock, cost_per_stock, total_cost, note
# ===========================

import pandas as pd
import datetime

def _table_exists(conn, table_name: str) -> bool:
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    return conn.execute(q, (table_name,)).fetchone() is not None

def _cols(conn, table_name: str):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [r[1] for r in rows]

def _require_stock_ledger_schema(conn):
    if not _table_exists(conn, "stock_ledger"):
        raise RuntimeError("stock_ledger table does not exist.")

    needed = {
        "id", "date", "ref_type", "ref_no", "material_code",
        "qty", "uom", "qty_stock", "cost_per_stock", "total_cost", "note"
    }
    have = set(_cols(conn, "stock_ledger"))
    missing = sorted(list(needed - have))
    if missing:
        raise RuntimeError(f"stock_ledger missing columns: {missing}. Found: {sorted(list(have))}")

def get_stock_on_hand(search: str = "") -> pd.DataFrame:
    """
    Shows ONLY items that are on the floor now (qty_stock != 0),
    using the latest row per material_code in stock_ledger.
    Joins to materials for name (materials table).
    """
    conn = get_conn()
    _require_stock_ledger_schema(conn)

    # materials table (you told me it's called materials)
    if not _table_exists(conn, "materials"):
        raise RuntimeError("materials table does not exist (expected table name: materials).")

    mcols = set(_cols(conn, "materials"))
    m_code = "material_code" if "material_code" in mcols else ("code" if "code" in mcols else None)
    m_name = "material_name" if "material_name" in mcols else ("name" if "name" in mcols else None)
    m_uom  = "uom" if "uom" in mcols else ("stock_uom" if "stock_uom" in mcols else None)

    if not m_code:
        raise RuntimeError(f"materials table must have material_code (or code). Found: {sorted(list(mcols))}")

    search = (search or "").strip().lower()

    # Latest row per material_code (by id)
    df = pd.read_sql(f"""
        WITH latest AS (
            SELECT material_code, MAX(id) AS max_id
            FROM stock_ledger
            GROUP BY material_code
        )
        SELECT
            l.material_code AS code,
            COALESCE(m.{m_name if m_name else m_code}, l.material_code) AS name,
            COALESCE(m.{m_uom if m_uom else 'NULL'}, l.uom, '') AS uom,
            l.qty_stock AS qty_on_hand,
            l.cost_per_stock AS avg_cost,
            ROUND(l.qty_stock * l.cost_per_stock, 2) AS stock_value
        FROM latest x
        JOIN stock_ledger l ON l.id = x.max_id
        LEFT JOIN materials m ON m.{m_code} = l.material_code
        WHERE ABS(l.qty_stock) > 0.00001
        ORDER BY name
    """, conn)

    if search:
        df = df[
            df["code"].astype(str).str.lower().str.contains(search)
            | df["name"].astype(str).str.lower().str.contains(search)
        ].copy()

    # nice rounding
    df["qty_on_hand"] = df["qty_on_hand"].astype(float).round(4)
    df["avg_cost"] = df["avg_cost"].astype(float).round(6)
    return df

def post_stock_adjustment(code: str, qty_delta: float, uom: str,
                          cost_per_uom: float = 0.0,
                          note: str = "",
                          ref_type: str = "MANUAL",
                          ref_no: str = ""):
    """
    Inserts a new stock_ledger row and updates qty_stock + cost_per_stock properly.

    Rules:
    - qty_delta > 0 (ADD): cost_per_stock becomes weighted average using cost_per_uom.
    - qty_delta < 0 (SUBTRACT): cost_per_stock stays the same; total_cost uses current avg cost.
    """
    conn = get_conn()
    _require_stock_ledger_schema(conn)

    code = (code or "").strip()
    if not code:
        raise ValueError("code is required.")
    if abs(qty_delta) < 0.0000001:
        raise ValueError("qty_delta cannot be zero.")
    if not uom:
        uom = ""

    # Get latest balance for this code
    row = conn.execute("""
        SELECT qty_stock, cost_per_stock
        FROM stock_ledger
        WHERE material_code = ?
        ORDER BY id DESC
        LIMIT 1
    """, (code,)).fetchone()

    prev_qty = float(row[0]) if row else 0.0
    prev_cost = float(row[1]) if row else 0.0

    new_qty = prev_qty + float(qty_delta)

    # Determine movement cost and new avg cost
    if qty_delta > 0:
        add_cost = float(cost_per_uom)
        move_cost = float(qty_delta) * add_cost

        # weighted avg if new_qty > 0, else 0
        if abs(new_qty) < 0.00001:
            new_cost = 0.0
        else:
            new_cost = ((prev_qty * prev_cost) + (float(qty_delta) * add_cost)) / new_qty
    else:
        # subtract at current avg cost
        move_cost = float(qty_delta) * prev_cost  # negative value
        new_cost = prev_cost

        # if stock hits 0, reset avg cost
        if abs(new_qty) < 0.00001:
            new_cost = 0.0

    today = datetime.date.today().isoformat()

    conn.execute("""
        INSERT INTO stock_ledger
        (date, ref_type, ref_no, material_code, qty, uom, qty_stock, cost_per_stock, total_cost, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        today, ref_type, ref_no, code,
        float(qty_delta), uom,
        float(new_qty), float(new_cost),
        float(move_cost), note or ""
    ))
    conn.commit()

def integrity_check_stock() -> pd.DataFrame:
    """
    Flags negative stock on hand (latest qty_stock < 0).
    """
    df = get_stock_on_hand("")
    bad = df[df["qty_on_hand"] < -0.00001].copy()
    return bad


def upsert_material(row: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO materials (
                material_code, material_name, category,
                stock_uom, issue_uom, issue_to_stock_factor,
                std_wastage_pct, is_critical, active, notes
            ) VALUES (
                :material_code, :material_name, :category,
                :stock_uom, :issue_uom, :issue_to_stock_factor,
                :std_wastage_pct, :is_critical, :active, :notes
            )
            ON CONFLICT(material_code) DO UPDATE SET
                material_name = excluded.material_name,
                category = excluded.category,
                stock_uom = excluded.stock_uom,
                issue_uom = excluded.issue_uom,
                issue_to_stock_factor = excluded.issue_to_stock_factor,
                std_wastage_pct = excluded.std_wastage_pct,
                is_critical = excluded.is_critical,
                active = excluded.active,
                notes = excluded.notes
        """, row)
        conn.commit()
