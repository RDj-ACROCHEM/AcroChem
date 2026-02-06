# app.py
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader
import copy
import pandas as pd
import logic
import db

# -----------------------------
# INIT
# -----------------------------
st.set_page_config(
    page_title="AcroChem Lite",
    page_icon="🧪",
    layout="wide"
)
logic.init_db()


# ================= LOGIN WALL =================

credentials = {
    "usernames": {
        username: {
            "name": user["name"],
            "password": user["password"],
        }
        for username, user in st.secrets["credentials"]["usernames"].items()
    }
}
st.image("acro_text.png", width=200)
st.markdown("### AcroChem_lite")
st.caption("")
st.markdown("---"
)
authenticator = stauth.Authenticate(
    credentials,
    st.secrets["cookie"]["name"],
    st.secrets["cookie"]["key"],
    st.secrets["cookie"]["expiry_days"],
)
authenticator.login("main")

authentication_status = st.session_state.get("authentication_status")
name = st.session_state.get("name")
username = st.session_state.get("username")


if authentication_status is False:
    st.error("❌ Wrong credentials")
    st.stop()

if authentication_status is None:
    st.info("Enter your username and password")
    st.stop()

# =================================================
db.init_db()
logic.init_db()

authenticator.logout("Logout", "sidebar")
st.sidebar.write(f"Logged in as {name}")
st.markdown("""
<style>
html, body, [data-testid="stApp"] {
    background-color: #0f0f0f;
}

.login-box {
    max-width: 420px;
    margin: 120px auto;
    padding: 40px;
    background: #111;
    border-radius: 12px;
    text-align: center;
}

.stButton > button {
    background-color: #c1121f;
    color: white;
    width: 100%;
    height: 45px;
    font-weight: bold;
}
</style>
""", unsafe_allow_html=True)


# -----------------------------
# NAV
# -----------------------------
with st.sidebar:
    st.image("acro_logo_clean.png", width=60)
    st.markdown("### Navigation")
    page = st.radio(
        "Go to",
        [
            "Materials (RM Master)",
            "Products",
            "Formulas",
            "Purchases",
            "Batches (Weekly Entry)",
            "Stock On Hand",
            "Stocktake & Variance",
            "Ledger (All Movements)",
            "Exports",
            "Thinners Sales",
        ],
    )

# -----------------------------
# Helpers
# -----------------------------
def _read_any_file(uploaded):
    if uploaded is None:
        return None
    name = uploaded.name.lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded)
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(uploaded)
    raise ValueError("Unsupported file type")

def _norm(s: str) -> str:
    return str(s or "").strip().lower()

def _materials_lookup_by_name():
    mats = logic.get_materials(active_only=False)
    mats["__name_norm"] = mats["material_name"].astype(str).str.strip().str.lower()
    # If duplicates exist, first match wins. Fix duplicates in RM Master.
    return mats.set_index("__name_norm")

def _get_products_df():
    with logic.get_conn() as conn:
        return pd.read_sql_query(
            "SELECT product_code, product_name, active FROM products ORDER BY product_name",
            conn
        )

def _get_formula_lines(product_code: str):
    with logic.get_conn() as conn:
        return pd.read_sql_query(
            """
            SELECT f.material_code, m.material_name, f.qty, f.uom
            FROM formulas f
            JOIN materials m ON m.material_code = f.material_code
            WHERE f.product_code = ?
            ORDER BY m.category, m.material_name
            """,
            conn,
            params=(product_code,)
        )

# =================================================
# RM MASTER PAGE
# =================================================
if page == "Materials (RM Master)":

    # -----------------------------
    # INIT SESSION STATE
    # -----------------------------
    if "rm_df" not in st.session_state:
        st.session_state.rm_df = pd.DataFrame(columns=[
            "material_code",
            "material_name",
            "category",
            "stock_uom",
            "issue_uom",
            "issue_to_stock_factor",
            "std_wastage_pct",
            "is_critical",
            "active",
            "notes",
        ])

    CATEGORY_OPTIONS = ["Resin", "Pigment", "Solvent", "Additive", "Packaging"]

    st.title("RM Master (Raw Materials)")

    # -----------------------------
    # IMPORT CSV / XLSX
    # -----------------------------
    st.subheader("Import Raw Materials (CSV / XLSX)")
    uploaded = st.file_uploader("Upload file", type=["csv", "xlsx"])

    if uploaded is not None:
        try:
            if uploaded.name.lower().endswith(".csv"):
                df = pd.read_csv(uploaded)
            else:
                df = pd.read_excel(uploaded)

            # Normalize column names
            df.columns = df.columns.str.strip().str.lower()

            # Must have these
            if "material_code" not in df.columns or "material_name" not in df.columns:
                st.error("Your file must contain columns: material_code, material_name")
            else:
                # Add missing columns with defaults
                defaults = {
                    "category": "",
                    "stock_uom": "kg",
                    "issue_uom": "kg",
                    "issue_to_stock_factor": 1.0,
                    "std_wastage_pct": 0.0,
                    "is_critical": 0,
                    "active": 1,
                    "notes": "imported",
                }
                for col, default in defaults.items():
                    if col not in df.columns:
                        df[col] = default

                # Force category blank on import (you choose later)
                df["category"] = ""

                # Clean strings
                df["material_code"] = df["material_code"].astype(str).str.strip()
                df["material_name"] = df["material_name"].astype(str).str.strip()

                st.write("Preview (category intentionally blank):")
                st.dataframe(df, use_container_width=True)

                if st.button("Import into RM Master", type="primary"):
                    st.session_state.rm_df = pd.concat(
                        [st.session_state.rm_df, df],
                        ignore_index=True
                    )

                    # Drop duplicate codes, keep latest
                    st.session_state.rm_df = (
                        st.session_state.rm_df
                        .sort_values(by=["material_code"])
                        .drop_duplicates(subset=["material_code"], keep="last")
                        .reset_index(drop=True)
                    )

                    st.success(f"Imported {len(df)} materials.")
                    st.rerun()

        except Exception as e:
            st.error(f"Import failed: {e}")

    st.divider()

    # -----------------------------
    # SEARCH
    # -----------------------------
    st.subheader("RM Master Table")
    search = st.text_input("Search by code or name")

    view_df = st.session_state.rm_df.copy()

    if search:
        view_df = view_df[
            view_df["material_code"].astype(str).str.contains(search, case=False, na=False)
            | view_df["material_name"].astype(str).str.contains(search, case=False, na=False)
        ]

    # -----------------------------
    # EDIT TABLE (CATEGORY AFTER IMPORT)
    # -----------------------------
    edited_df = st.data_editor(
        view_df,
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "category": st.column_config.SelectboxColumn(
                "Category",
                options=CATEGORY_OPTIONS,
            ),
            "is_critical": st.column_config.CheckboxColumn("Critical"),
            "active": st.column_config.CheckboxColumn("Active"),
        },
        disabled=["material_code"],
        key="rm_editor"
    )

    col1, col2 = st.columns(2)

    # -----------------------------
    # SAVE CHANGES
    # -----------------------------
    with col1:
        if st.button("Save changes"):
            # Update original df by matching material_code
            base = st.session_state.rm_df.set_index("material_code")
            new = edited_df.set_index("material_code")
            base.update(new)
            st.session_state.rm_df = base.reset_index()
            st.success("Saved.")

    # -----------------------------
    # DELETE
    # -----------------------------
    with col2:
        to_delete = st.multiselect(
            "Delete by MaterialCode",
            options=st.session_state.rm_df["material_code"].tolist()
        )
        if st.button("DELETE selected"):
            st.session_state.rm_df = st.session_state.rm_df[
                ~st.session_state.rm_df["material_code"].isin(to_delete)
            ].reset_index(drop=True)
            st.success(f"Deleted {len(to_delete)} rows.")
            st.rerun()# ============================
# PRODUCTS
# ============================
elif page == "Products":

    st.title("Products Master")

    # -------------------------
    # LOAD PRODUCTS
    # -------------------------
    df = logic.get_all_products_df()

    # -------------------------
    # SEARCH
    # -------------------------
    search = st.text_input("Search product")

    if search:
        df = df[df["product_name"].str.contains(search, case=False, na=False)]

    st.dataframe(df, use_container_width=True)

    # -------------------------
    # IMPORT PRODUCTS
    # -------------------------
    st.subheader("Import Products")

    uploaded = st.file_uploader("Upload Excel (.xlsx)", type=["xlsx"])

    if uploaded:
        import_df = pd.read_excel(uploaded)

        first_col = import_df.columns[0]

        st.info(f"Reading column: {first_col}")

        if st.button("Import products"):
            for val in import_df[first_col].dropna():
                logic.save_product({
                    "product_code": str(val).strip(),
                    "product_name": str(val).strip(),
                    "category": "Unassigned",
                    "active": 1
                })

            st.success("Products imported as Unassigned")
            st.experimental_rerun()

    # -------------------------
    # EDIT CATEGORY (AFTER IMPORT)
    # -------------------------
    st.subheader("Assign Category")

    if not df.empty:
        selected = st.selectbox(
            "Select product",
            df["product_code"]
        )

        cat = st.selectbox(
            "Category",
            ["Solvents", "Paints", "Base", "Unassigned"]
        )

        if st.button("Update Category"):
            logic.save_product({
                "product_code": selected,
                "product_name": df[df["product_code"] == selected]["product_name"].values[0],
                "category": cat,
                "active": 1
            })
            st.success("Category updated")
            st.experimental_rerun()

    # -------------------------
    # DELETE PRODUCT
    # -------------------------
    st.subheader("Delete Product (Permanent)")

    del_code = st.selectbox(
        "Select product to delete",
        df["product_code"] if not df.empty else []
    )

    if st.button("Delete product"):
        logic.delete_product(del_code)
        st.success("Product deleted")
        st.experimental_rerun()

# =========================================================
# 3) FORMULAS (BOM)
# =========================================================
elif page == "Formulas":

    st.title("Formulas (BOM)")

    # ===============================
    # SELECT PRODUCT
    # ===============================
    products_df = logic.get_products(active_only=True)

    if products_df.empty:
        st.warning("No products available.")
        st.stop()

    product_map = dict(zip(products_df["product_name"], products_df["product_code"]))
    product_name = st.selectbox("Select Product", list(product_map.keys()))
    product_code = product_map[product_name]

    st.divider()

    # ===============================
    # ADD MATERIAL TO FORMULA
    # ===============================
    st.subheader("Add material to formula")

    materials_df = logic.get_materials(active_only=True)

    if materials_df.empty:
        st.warning("No raw materials available.")
        st.stop()

    material_map = dict(zip(materials_df["material_name"], materials_df["material_code"]))

    col1, col2, col3, col4 = st.columns([4, 2, 2, 2])

    with col1:
        material_name = st.selectbox(
            "Raw material",
            [""] + list(material_map.keys())
        )

    with col2:
        qty = st.number_input(
            "Quantity per batch",
            min_value=0.0,
            step=0.01,
            format="%.3f"
        )

    with col3:
        uom = st.selectbox("UOM", ["kg", "l", "ea"])

    with col4:
        add_btn = st.button("Add material")

    if add_btn:
        if not material_name or qty <= 0:
            st.error("Select a material and enter a quantity > 0")
        else:
            try:
                logic.add_formula_line(
                    product_code=product_code,
                    material_code=material_map[material_name],
                    qty=qty,
                    uom=uom
                )
                st.success("Material added")
                st.experimental_rerun()
            except Exception as e:
                st.error(str(e))

    st.divider()

    # ===============================
    # CURRENT FORMULA
    # ===============================
    st.subheader("Current formula")

    formula_df = logic.get_formula(product_code)

    if formula_df.empty:
        st.info("No materials added yet.")
    else:
        for _, row in formula_df.iterrows():
            c1, c2, c3, c4 = st.columns([4, 2, 2, 1])

            c1.write(row["material_name"])
            c2.write(f'{row["qty"]:.3f}')
            c3.write(row["uom"])

            if c4.button("❌", key=f"del_{row['id']}"):
                logic.delete_formula_line(row["id"])
                st.experimental_rerun()
# =========================================================
# 4) PURCHASES (stock IN)
# =========================================================
elif page == "Purchases":
    st.header("Purchases (Stock IN)")

    mats = logic.get_materials(active_only=True)
    if mats.empty:
        st.warning("Add materials first.")
    else:
        st.subheader("Manual purchase entry")
        c1, c2, c3, c4 = st.columns(4)
        mat_choice = c1.selectbox(
            "Material",
            (mats["material_code"] + " — " + mats["material_name"]).tolist(),
            key="pur_mat_choice"
        )
        material_code = mat_choice.split(" — ")[0].strip()

        qty_stock = c2.number_input("Qty IN (STOCK UoM)", min_value=0.0, value=0.0, step=1.0, key="pur_qty")
        total_cost = c3.number_input("Total Cost (ZAR)", min_value=0.0, value=0.0, step=1.0, key="pur_cost")
        ref_no = c4.text_input("Ref (invoice/grv)", value="", key="pur_ref")

        if st.button("Post Purchase (Stock IN)", type="primary", key="pur_post"):
            if qty_stock <= 0:
                st.error("Qty must be > 0")
            elif total_cost <= 0:
                st.error("Total cost must be > 0 (needed for costing).")
            else:
                logic.receive_purchase(material_code, qty_stock, total_cost, ref_no=ref_no.strip() or None)
                st.success("Purchase posted.")

    st.divider()
    st.subheader("Import purchases (CSV/XLSX)")

    st.caption("Required columns (any case): material_name, qty_stock, total_cost. Optional: ref_no")
    up = st.file_uploader("Upload purchases file", type=["csv","xlsx","xls"], key="pur_upload")

    if up:
        try:
            df = _read_any_file(up)
            df.columns = [c.strip().lower() for c in df.columns]
            st.dataframe(df.head(50), use_container_width=True)

            needed = {"material_name", "qty_stock", "total_cost"}
            if not needed.issubset(set(df.columns)):
                st.error(f"Missing columns. Need at least: {sorted(list(needed))}")
            else:
                if st.button("Import & Post Purchases", type="primary", key="pur_import"):
                    lut = _materials_lookup_by_name()
                    posted = 0
                    skipped = 0
                    missing_names = []

                    for _, r in df.iterrows():
                        name = _norm(r.get("material_name"))
                        if not name:
                            skipped += 1
                            continue
                        if name not in lut.index:
                            missing_names.append(r.get("material_name"))
                            skipped += 1
                            continue

                        code = lut.loc[name, "material_code"]
                        q = float(r.get("qty_stock") or 0)
                        tc = float(r.get("total_cost") or 0)
                        rn = str(r.get("ref_no") or "").strip() or None

                        if q <= 0 or tc <= 0:
                            skipped += 1
                            continue

                        logic.receive_purchase(code, q, tc, ref_no=rn)
                        posted += 1

                    st.success(f"Posted: {posted} | Skipped: {skipped}")
                    if missing_names:
                        st.warning("These material names were not found in RM Master (fix RM Master names or import them first):")
                        st.write(sorted(set(missing_names)))
        except Exception as e:
            st.error(str(e))

# =========================================================
# 5) BATCHES (stock OUT via formula)
# =========================================================
elif page == "Batches (Weekly Entry)":
    st.header("Batches (Manufacturing)")

    products = _get_products_df()
    if products.empty:
        st.warning("Add products first.")
    else:
        prod_choice = st.selectbox(
            "Product",
            (products["product_code"] + " — " + products["product_name"]).tolist(),
            key="bat_prod_choice"
        )
        product_code = prod_choice.split(" — ")[0].strip()

        formula = _get_formula_lines(product_code)
        if formula.empty:
            st.warning("No formula saved for this product. Go to Formulas page and save one.")
        else:
            c1, c2, c3 = st.columns(3)
            batch_multiplier = c1.number_input(
                "Batch multiplier (1 = one standard batch)",
                min_value=0.01, value=1.0, step=0.1, key="bat_mult"
            )
            ref_no = c2.text_input("Batch ref (week/job/card)", value="", key="bat_ref")
            note = c3.text_input("Note", value="", key="bat_note")

            # Compute consumption in STOCK units
            preview = formula.copy()
            preview["qty_stock_to_issue"] = preview["qty"].astype(float) * float(batch_multiplier)

            st.subheader("Expected consumption (STOCK UoM)")
            st.dataframe(preview[["material_code","material_name","qty_stock_to_issue","uom"]], use_container_width=True)

            if st.button("POST Batch Consumption (Stock OUT)", type="primary", key="bat_post"):
                posted = 0
                for _, r in preview.iterrows():
                    mc = str(r["material_code"]).strip()
                    q_issue = float(r["qty_stock_to_issue"] or 0)
                    if q_issue <= 0:
                        continue
                    # Issue stock at weighted avg cost
                    logic.issue_stock(mc, q_issue, ref_type="BATCH", ref_no=ref_no.strip() or None)
                    posted += 1
                st.success(f"Posted batch consumption lines: {posted}")

# =========================================================
# 6) STOCK ON HAND
# =========================================================
elif page == "Stock On Hand":
    import streamlit as st
    import logic

    st.title("Stock On Hand")

    search = st.text_input("Search (code or name)", "")

    try:
        df = logic.get_stock_on_hand(search=search)
    except Exception as e:
        st.error(f"Stock On Hand failed: {e}")
        st.stop()

    st.dataframe(df, use_container_width=True)

    st.divider()

    with st.expander("Integrity check (negative stock)"):
        bad = logic.integrity_check_stock()
        if bad.empty:
            st.success("No negative stock found ✅")
        else:
            st.error("Negative stock found (fix this) ❌")
            st.dataframe(bad, use_container_width=True)

    st.divider()
    st.subheader("Manual Add / Subtract (writes to ledger)")

    c1, c2, c3 = st.columns(3)
    with c1:
        code = st.text_input("Material code", "")
    with c2:
        uom = st.text_input("UOM", "")
    with c3:
        qty_delta = st.number_input("Qty change (+ add, - subtract)", value=0.0, step=1.0, format="%.4f")

    cost_per_uom = st.number_input("Cost per UOM (ONLY for ADD)", value=0.0, step=0.01, format="%.6f")
    note = st.text_input("Note (optional)", "")
    ref_no = st.text_input("Ref no (optional)", "")

    if st.button("Post adjustment"):
        try:
            logic.post_stock_adjustment(
                code=code.strip(),
                qty_delta=float(qty_delta),
                uom=uom.strip(),
                cost_per_uom=float(cost_per_uom),
                note=note.strip(),
                ref_type="MANUAL",
                ref_no=ref_no.strip()
            )
            st.success("Posted ✅")
            st.experimental_rerun()
        except Exception as e:
            st.error(f"Failed: {e}")
# =========================================================
# 7) STOCKTAKE & VARIANCE (Upload material_name + counted_qty)
# =========================================================
elif page == "Stocktake & Variance":
    st.header("Stocktake & Variance")

    st.caption("Upload a CSV/XLSX with columns: material_name, counted_qty (or physical_qty). Category optional.")

    up = st.file_uploader("Upload stocktake file", type=["csv","xlsx","xls"], key="stocktake_upload")

    df_in = None
    if up:
        try:
            df_in = _read_any_file(up)
            df_in.columns = [c.strip().lower() for c in df_in.columns]

            # Accept either counted_qty or physical_qty
            if "counted_qty" not in df_in.columns and "physical_qty" in df_in.columns:
                df_in["counted_qty"] = df_in["physical_qty"]

            needed = {"material_name", "counted_qty"}
            if not needed.issubset(set(df_in.columns)):
                st.error(f"Missing columns. Need at least: {sorted(list(needed))}")
                df_in = None
            else:
                st.dataframe(df_in.head(100), use_container_width=True)
        except Exception as e:
            st.error(str(e))
            df_in = None

    st.divider()

    # Optional search (in the imported df)
    search = st.text_input("Search within imported stocktake (material name contains):", value="", key="stk_search")

    if df_in is not None:
        df_show = df_in.copy()
        if search.strip():
            df_show = df_show[df_show["material_name"].astype(str).str.contains(search.strip(), case=False, na=False)]

        st.subheader("Editable stocktake grid (you can fix names/qty here before posting)")
        edit = pd.DataFrame({
            "material_name": df_show["material_name"].astype(str),
            "counted_qty": pd.to_numeric(df_show["counted_qty"], errors="coerce").fillna(0.0),
            "category": df_show["category"] if "category" in df_show.columns else ""
        })

        edited = st.data_editor(edit, use_container_width=True, num_rows="dynamic", key="stk_editor")

        if st.button("POST Stocktake (creates variance adjustments)", type="primary", key="stk_post"):
            lut = _materials_lookup_by_name()
            posted = 0
            skipped = 0
            missing = []

            for _, r in edited.iterrows():
                name = _norm(r.get("material_name"))
                if not name:
                    skipped += 1
                    continue
                qty = float(r.get("counted_qty") or 0)
                if qty < 0:
                    skipped += 1
                    continue

                if name not in lut.index:
                    missing.append(r.get("material_name"))
                    skipped += 1
                    continue

                code = lut.loc[name, "material_code"]
                logic.post_stocktake(code, qty, note="Imported stocktake")
                posted += 1

            st.success(f"Posted: {posted} | Skipped: {skipped}")
            if missing:
                st.warning("These names were not found in RM Master. Fix spelling in RM Master or in the uploaded file:")
                st.write(sorted(set(missing)))

    else:
        st.info("Upload a stocktake file to continue.")

# =========================================================
# 8) LEDGER (All Movements)
# =========================================================
elif page == "Ledger (All Movements)":
    st.header("Ledger (All Movements)")
    with logic.get_conn() as conn:
        df = pd.read_sql_query(
            "SELECT * FROM stock_ledger ORDER BY date DESC LIMIT 2000",
            conn
        )
    st.dataframe(df, use_container_width=True)

# =========================================================
# 9) EXPORTS
# =========================================================
elif page == "Exports":
    st.header("Exports")
    st.subheader("Stock On Hand")
    df_stock = logic.stock_on_hand()
    st.dataframe(df_stock, use_container_width=True)

    csv1 = df_stock.to_csv(index=False).encode("utf-8")
    st.download_button("Download stock_on_hand.csv", data=csv1, file_name="stock_on_hand.csv", mime="text/csv")

    st.subheader("Ledger (last 2000)")
    with logic.get_conn() as conn:
        df_led = pd.read_sql_query("SELECT * FROM stock_ledger ORDER BY date DESC LIMIT 2000", conn)

    st.dataframe(df_led, use_container_width=True)
    csv2 = df_led.to_csv(index=False).encode("utf-8")
    st.download_button("Download ledger.csv", data=csv2, file_name="ledger.csv", mime="text/csv")

# =========================================================
# 10) Thinners sales
# =========================================================

elif page == "Thinners Sales":
    import pandas as pd
    import streamlit as st

    # Make sure thinner tables exist before any query
    logic.ensure_thinners_schema()

    st.title("Thinners: Recipes + Sales")

    # ----------------------------
    # A) THINNER RECIPE (like BOM)
    # ----------------------------
    st.subheader("A) Thinner Recipe (like Formulas / BOM)")

    # Let user type a thinner name/code (simple + flexible)
    thinner_code = st.text_input("Thinner name / code (e.g. DTM Blend, QD Blend)", value="").strip()

    materials_df = logic.get_materials_lookup(active_only=True)

    if "thin_lines" not in st.session_state:
        st.session_state.thin_lines = []

    # Load existing recipe into session if user clicks
    colA, colB = st.columns([1, 1])
    with colA:
        if st.button("Load saved recipe into editor"):
            if not thinner_code:
                st.warning("Type a thinner name/code first.")
            else:
                existing = logic.get_thinner_recipe_df(thinner_code)
                st.session_state.thin_lines = []
                for _, r in existing.iterrows():
                    st.session_state.thin_lines.append({
                        "material_code": r["material_code"],
                        "material_name": r["material_name"],
                        "qty_issue": float(r["qty_issue"]),
                        "uom": r["uom"]
                    })
                st.success("Loaded.")

    with colB:
        if st.button("Clear editor"):
            st.session_state.thin_lines = []

    st.markdown("### Add material to recipe")
    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])

    # Dropdown label that never shows nan
    materials_df["label"] = materials_df.apply(
        lambda r: f"{r['material_code']} — {r['material_name']}",
        axis=1
    )

  # Make sure materials exist
if materials_df.empty:
    st.warning("No raw materials available")
    st.stop()

# Use material_code (THIS EXISTS)
material_options = materials_df["material_code"].tolist()

pick_label = st.selectbox(
    "Raw material",
    material_options
)

# SAFE lookup (NO CRASH)
filtered = materials_df[materials_df["material_code"] == pick_label]

if filtered.empty:
    st.warning("Please select a raw material")
    st.stop()

pick_row = filtered.iloc[0]    with col2:
        qty_issue = st.number_input("Qty per 1 unit (issue)", min_value=0.0, value=0.0, step=0.01)
    with col3:
        uom = st.selectbox("UOM", ["L", "kg", "g", "ml"])
    with col4:
        if st.button("Add line"):
            if not thinner_code:
                st.error("Type a thinner name/code first.")
            elif qty_issue <= 0:
                st.error("Qty must be > 0")
            else:
                # prevent duplicates: update qty if already exists
                found = False
                for ln in st.session_state.thin_lines:
                    if ln["material_code"] == pick_code:
                        ln["qty_issue"] = float(qty_issue)
                        ln["uom"] = uom
                        found = True
                        break
                if not found:
                    st.session_state.thin_lines.append({
                        "material_code": pick_code,
                        "material_name": pick_name,
                        "qty_issue": float(qty_issue),
                        "uom": uom
                    })

    # Show only what's in the recipe
    if st.session_state.thin_lines:
        recipe_view = pd.DataFrame(st.session_state.thin_lines)
        st.dataframe(recipe_view, use_container_width=True)

        # Delete one line
        del_pick = st.selectbox(
            "Delete a line",
            recipe_view["material_code"].tolist()
        )
        if st.button("Delete selected line"):
            st.session_state.thin_lines = [ln for ln in st.session_state.thin_lines if ln["material_code"] != del_pick]
            st.success("Deleted.")

        if st.button("Save recipe"):
            lines = [{"material_code": ln["material_code"], "qty_issue": ln["qty_issue"], "uom": ln["uom"]}
                     for ln in st.session_state.thin_lines]
            logic.upsert_thinner_recipe(thinner_code, lines)
            st.success("Recipe saved.")
    else:
        st.info("No lines in this recipe yet. Add materials above.")

    st.divider()

    # ----------------------------
    # B) RECORD THINNER SALE (deduct stock)
    # ----------------------------
    st.subheader("B) Record Thinner Sale (auto-deduct raw materials from stock)")

    # Show list of thinners that have recipes
    thinners = pd.read_sql("""
        SELECT DISTINCT thinner_code
        FROM thinner_recipes
        ORDER BY thinner_code
    """, logic.get_conn())

    thinner_list = thinners["thinner_code"].tolist() if not thinners.empty else []

    if thinner_list:
        sale_thinner = st.selectbox("Thinner sold", thinner_list)
    else:
        sale_thinner = ""
        st.warning("No thinner recipes exist yet. Save a recipe first.")

    colS1, colS2, colS3 = st.columns([2, 2, 3])
    with colS1:
        sale_date = st.date_input("Sale date")
    with colS2:
        qty_sold = st.number_input("Qty sold", min_value=0.0, value=0.0, step=1.0)
    with colS3:
        sale_notes = st.text_input("Notes (optional)", value="")

    if st.button("Save sale + deduct stock"):
        if not sale_thinner:
            st.error("Select a thinner.")
        elif qty_sold <= 0:
            st.error("Qty sold must be > 0.")
        else:
            logic.record_thinner_sale_and_deduct_stock(
                sale_date=sale_date,
                thinner_code=sale_thinner,
                qty_sold=qty_sold,
                uom="L",
                notes=sale_notes
            )
            st.success("Saved sale and deducted raw materials from stock.")

    st.markdown("### Recent thinner sales")
    st.dataframe(logic.get_thinner_sales_df(limit=200), use_container_width=True)

elif page == "stock_on_hand":
    import streamlit as st
    import pandas as pd
    import logic

    st.title("Stock On Hand")

    # Search
    search = st.text_input("Search (code or name)", "")

    # Pull current stock on hand (ONLY items with non-zero balance)
    try:
        df = logic.get_stock_on_hand(search=search)
    except Exception as e:
        st.error(f"Stock On Hand failed: {e}")
        st.stop()

    st.dataframe(df, use_container_width=True)

    st.divider()

    # Integrity check (negative stock)
    with st.expander("Integrity check (negative stock)"):
        bad = logic.integrity_check_stock()
        if bad.empty:
            st.success("No negative stock found ✅")
        else:
            st.error("Negative stock found (this must be fixed) ❌")
            st.dataframe(bad, use_container_width=True)

    st.divider()

    st.subheader("Manual Add / Subtract (writes to ledger)")

    col1, col2, col3 = st.columns(3)
    with col1:
        item_type = st.selectbox("Item type", ["RM", "FG"])
    with col2:
        code = st.text_input("Code (exact)", "")
    with col3:
        qty_delta = st.number_input("Qty change (+ add, - subtract)", value=0.0, step=1.0, format="%.4f")

    col4, col5 = st.columns(2)
    with col4:
        cost_per_uom = st.number_input("Cost per UOM (optional)", value=0.0, step=0.01, format="%.6f")
    with col5:
        notes = st.text_input("Notes (optional)", "")

    if st.button("Post adjustment"):
        if not code.strip():
            st.warning("Enter a code first.")
        elif abs(qty_delta) < 0.0000001:
            st.warning("Qty change cannot be zero.")
        else:
            try:
                logic.post_stock_adjustment(
                    item_type=item_type,
                    code=code.strip(),
                    qty_delta=float(qty_delta),
                    cost_per_uom=float(cost_per_uom),
                    notes=notes.strip(),
                    ref_type="MANUAL",
                    ref_id=""
                )
                st.success("Posted to ledger ✅")
                st.experimental_rerun()
            except Exception as e:
                st.error(f"Failed to post adjustment: {e}")
