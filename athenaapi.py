import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import streamlit as st
import os

st.set_page_config(layout="wide")

VALID_USERNAME = os.getenv("VALID_USERNAME")
VALID_PASSWORD = os.getenv("VALID_PASSWORD")


# ---------------------------
# Utility functions
# ---------------------------

def get_token(client_id: str, client_secret: str, token_url: str) -> str:
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    data = {"grant_type": "client_credentials"}
    auth = HTTPBasicAuth(client_id, client_secret)

    resp = requests.post(token_url, headers=headers, data=data, auth=auth, timeout=60)
    resp.raise_for_status()
    payload = resp.json()
    if "access_token" not in payload:
        raise RuntimeError(f"Unexpected token response: {payload}")
    return payload["access_token"]


def get_details(url: str, token: str) -> object:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def flatten_all_json(raw_json: object, sep: str = "_") -> pd.DataFrame:
    if raw_json is None:
        return pd.DataFrame()

    df = pd.json_normalize(raw_json, sep=sep)

    while True:
        list_cols = [
            col for col in df.columns
            if df[col].apply(lambda x: isinstance(x, list)).any()
        ]
        if not list_cols:
            break

        for col in list_cols:
            df = df.explode(col).reset_index(drop=True)
            df = pd.json_normalize(df.to_dict(orient="records"), sep=sep)

    return df


def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


# ---------------------------
# Pricing parsers
# ---------------------------

def parse_completion_design_items(raw, project_number, well_id) -> pd.DataFrame:
    rows = []
    designs = raw if isinstance(raw, list) else []
    for d in designs:
        proppants = d.get("proppantsTypeMesh", []) if isinstance(d, dict) else []
        for p in proppants:
            rows.append({
                "project_number": project_number,
                "well_id": str(well_id),
                "source": "completionDesign",
                "item_code": p.get("proppantSizeCatalogExternal"),
                "name": p.get("proppantCommercialName"),
                "uom": p.get("unit"),
                "unit_price": _f(p.get("unitPrice")),
                "discount_percentage": _f(p.get("discountPercentage")),
                "discounted_unit_price": _f(p.get("discountedUnitPrice")),
                "quantity": _f(p.get("quotedQuantity")),
            })
    return pd.DataFrame(rows)


def parse_frac_chemicals_items(raw, project_number, well_id) -> pd.DataFrame:
    rows = []
    groups = raw if isinstance(raw, list) else []
    for grp in groups:
        chem_types = grp.get("chemTypes", []) if isinstance(grp, dict) else []
        for ch in chem_types:
            rows.append({
                "project_number": project_number,
                "well_id": str(well_id),
                "source": "fracChemicals",
                "item_code": ch.get("chemicalTypeCatalogExternal"),
                "name": ch.get("commercialName"),
                "uom": ch.get("unit"),
                "unit_price": _f(ch.get("unitPrice")),
                "discount_percentage": _f(ch.get("discount")),
                "discounted_unit_price": _f(ch.get("discountedUnitPrice")),
                "quantity": _f(ch.get("quotedQuantity")),
            })
    return pd.DataFrame(rows)


def parse_cartage_charges_items(raw, project_number, well_id) -> pd.DataFrame:
    rows = []
    items = raw if isinstance(raw, list) else []
    for it in items:
        mu = it.get("measurementUnits") if isinstance(it, dict) else {}
        rows.append({
            "project_number": project_number,
            "well_id": str(well_id),
            "source": "cartageCharges",
            "item_code": it.get("cartageChargeCatalogExternal"),
            "name": it.get("itemDescription"),
            "uom": (mu or {}).get("label"),
            "unit_price": _f(it.get("unitPrice")),
            "discount_percentage": _f(it.get("discountPercentage")),
            "discounted_unit_price": _f(it.get("discountedUnitPrice")),
            "quantity": _f(it.get("quotedQuantity")),
        })
    return pd.DataFrame(rows)


def parse_service_charges_items(raw, project_number, well_id) -> pd.DataFrame:
    rows = []
    items = raw if isinstance(raw, list) else []
    for it in items:
        mu = it.get("measurementUnits") if isinstance(it, dict) else {}
        rows.append({
            "project_number": project_number,
            "well_id": str(well_id),
            "source": "serviceCharges",
            "item_code": it.get("serviceChargeCatalogExternal"),
            "name": it.get("itemDescription"),
            "uom": (mu or {}).get("label"),
            "unit_price": _f(it.get("unitPrice")),
            "discount_percentage": _f(it.get("discountPercentage")),
            "discounted_unit_price": _f(it.get("discountedUnitPrice")),
            "quantity": _f(it.get("quotedQuantity")),
        })
    return pd.DataFrame(rows)


# ---------------------------
# Well name extraction
# ---------------------------

def extract_well_name(raw_general_well_info) -> str:
    if isinstance(raw_general_well_info, list) and raw_general_well_info:
        first = raw_general_well_info[0]
        if isinstance(first, dict):
            return first.get("wellName")
    return None


# ---------------------------
# Project extraction
# ---------------------------

def build_project_dataset(project_number: str, token: str, base_url: str):
    project_url = f"{base_url}/project/?project_number={project_number}"
    raw_project = get_details(project_url, token)
    df_project = pd.json_normalize(raw_project, sep="_")

    if "wellIDs" in df_project.columns:
        df_exploded = df_project[["wellIDs"]].explode("wellIDs").reset_index(drop=True)
        df_wells = pd.json_normalize(df_exploded["wellIDs"])
    else:
        df_wells = pd.DataFrame()

    debug_attributes = [
        "generalWellInformation",
        "completionDesign",
        "fracChemicals",
        "cartageCharges",
        "serviceCharges",
    ]

    pricing_frames = []
    well_id_to_name = {}
    raw_by_well = {}

    if not df_wells.empty and "id" in df_wells.columns:
        for wid in df_wells["id"].dropna().unique():
            wid = str(wid)
            raw_by_well[wid] = {}

            for att in debug_attributes:
                api_url = f"{base_url}/{att}?well_id={wid}"
                raw = get_details(api_url, token)
                raw_by_well[wid][att] = raw

                if att == "generalWellInformation":
                    well_id_to_name[wid] = extract_well_name(raw)

                if att == "completionDesign":
                    pricing_frames.append(parse_completion_design_items(raw, project_number, wid))
                elif att == "fracChemicals":
                    pricing_frames.append(parse_frac_chemicals_items(raw, project_number, wid))
                elif att == "cartageCharges":
                    pricing_frames.append(parse_cartage_charges_items(raw, project_number, wid))
                elif att == "serviceCharges":
                    pricing_frames.append(parse_service_charges_items(raw, project_number, wid))

    df_items = (
        pd.concat([x for x in pricing_frames if x is not None and not x.empty], ignore_index=True)
        if pricing_frames else pd.DataFrame()
    )

    if not df_items.empty:
        df_items["well_name"] = df_items["well_id"].map(well_id_to_name)
        df_items["extended_discounted"] = df_items["discounted_unit_price"].fillna(0) * df_items["quantity"].fillna(0)
        df_items["extended_list"] = df_items["unit_price"].fillna(0) * df_items["quantity"].fillna(0)

        col_order = [
            "project_number", "well_id", "well_name", "source",
            "item_code", "name", "uom",
            "unit_price", "discount_percentage", "discounted_unit_price",
            "quantity", "extended_discounted", "extended_list"
        ]
        df_items = df_items[[c for c in col_order if c in df_items.columns]]

    return {
        "raw_project": raw_project,
        "df_project": df_project,
        "df_wells": df_wells,
        "df_items": df_items,
        "well_id_to_name": well_id_to_name,
        "raw_by_well": raw_by_well,
    }


# ---------------------------
# Comparison helpers
# ---------------------------

def normalize_compare_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "well_name", "source", "item_code", "name", "uom",
            "unit_price", "discount_percentage", "discounted_unit_price",
            "quantity", "extended_discounted", "extended_list"
        ])

    keep_cols = [
        "well_name", "source", "item_code", "name", "uom",
        "unit_price", "discount_percentage", "discounted_unit_price",
        "quantity", "extended_discounted", "extended_list"
    ]

    existing_cols = [c for c in keep_cols if c in df.columns]
    out = df[existing_cols].copy()

    for col in out.columns:
        if out[col].dtype == "object":
            out[col] = out[col].fillna("").astype(str).str.strip()
        else:
            out[col] = out[col]

    return out


def compare_projects(df1: pd.DataFrame, df2: pd.DataFrame, project1: str, project2: str):
    left = normalize_compare_df(df1)
    right = normalize_compare_df(df2)

    key_cols = ["well_name", "source", "item_code", "name", "uom"]
    value_cols = [
        "unit_price", "discount_percentage", "discounted_unit_price",
        "quantity", "extended_discounted", "extended_list"
    ]

    left_grouped = (
        left.groupby(key_cols, dropna=False, as_index=False)[value_cols]
        .sum(min_count=1)
    ) if not left.empty else pd.DataFrame(columns=key_cols + value_cols)

    right_grouped = (
        right.groupby(key_cols, dropna=False, as_index=False)[value_cols]
        .sum(min_count=1)
    ) if not right.empty else pd.DataFrame(columns=key_cols + value_cols)

    merged = left_grouped.merge(
        right_grouped,
        on=key_cols,
        how="outer",
        suffixes=(f"_{project1}", f"_{project2}"),
        indicator=True
    )

    def row_status(row):
        if row["_merge"] == "left_only":
            return f"Only in {project1}"
        if row["_merge"] == "right_only":
            return f"Only in {project2}"

        all_match = True
        for col in value_cols:
            v1 = row.get(f"{col}_{project1}")
            v2 = row.get(f"{col}_{project2}")

            if pd.isna(v1) and pd.isna(v2):
                continue

            v1 = 0 if pd.isna(v1) else round(float(v1), 6)
            v2 = 0 if pd.isna(v2) else round(float(v2), 6)

            if v1 != v2:
                all_match = False
                break

        return "Matched" if all_match else "Different values"

    if not merged.empty:
        merged["comparison_status"] = merged.apply(row_status, axis=1)
    else:
        merged["comparison_status"] = []

    matched = merged[merged["comparison_status"] == "Matched"].copy()
    different = merged[merged["comparison_status"] == "Different values"].copy()
    only_left = merged[merged["comparison_status"] == f"Only in {project1}"].copy()
    only_right = merged[merged["comparison_status"] == f"Only in {project2}"].copy()

    return merged, matched, different, only_left, only_right


# ---------------------------
# Login Page
# ---------------------------

def login():
    st.title("Login")

    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Login")

        if submit:
            if username == VALID_USERNAME and password == VALID_PASSWORD:
                st.session_state.logged_in = True
                st.rerun()
            else:
                st.error("Invalid username or password.")


# ---------------------------
# UI Renderers
# ---------------------------

def render_single_project(project_data, project_number):
    st.subheader(f"Project Details: {project_number}")

    st.write("Raw Project JSON:")
    st.write(project_data["raw_project"])

    st.write("Project Details:")
    st.dataframe(project_data["df_project"].T)

    st.write("Well IDs:")
    st.dataframe(project_data["df_wells"])

    st.subheader("Well Attribute Details")
    raw_by_well = project_data["raw_by_well"]

    for wid, attrs in raw_by_well.items():
        with st.expander(f"Well ID: {wid}", expanded=False):
            for att, raw in attrs.items():
                st.markdown(f"**{att}**")
                st.write(raw)
                df_flat = flatten_all_json(raw)
                st.dataframe(df_flat)

    df_items = project_data["df_items"]

    if not df_items.empty:
        st.subheader("All Pricing Items (All Wells)")
        st.dataframe(df_items, use_container_width=True)

        st.subheader("Totals by Well + Source")
        df_summary = (
            df_items
            .groupby(["well_id", "well_name", "source"], as_index=False)
            .agg(
                lines=("item_code", "size"),
                total_qty=("quantity", "sum"),
                total_discounted=("extended_discounted", "sum"),
                total_list=("extended_list", "sum"),
            )
        )
        st.dataframe(df_summary, use_container_width=True)
    else:
        st.info("No pricing items found (all pricing arrays empty).")


def render_compare_projects(project1, project2, data1, data2):
    st.subheader(f"Compare Projects: {project1} vs {project2}")

    col1, col2 = st.columns(2)

    with col1:
        st.markdown(f"### Project {project1}")
        st.dataframe(data1["df_project"].T, use_container_width=True)
        st.markdown("**Well IDs**")
        st.dataframe(data1["df_wells"], use_container_width=True)

    with col2:
        st.markdown(f"### Project {project2}")
        st.dataframe(data2["df_project"].T, use_container_width=True)
        st.markdown("**Well IDs**")
        st.dataframe(data2["df_wells"], use_container_width=True)

    merged, matched, different, only_left, only_right = compare_projects(
        data1["df_items"], data2["df_items"], project1, project2
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Matched Rows", len(matched))
    c2.metric("Different Values", len(different))
    c3.metric(f"Only in {project1}", len(only_left))
    c4.metric(f"Only in {project2}", len(only_right))

    st.markdown("### Full Comparison")
    st.dataframe(merged, use_container_width=True)

    st.markdown("### Matched")
    st.dataframe(matched, use_container_width=True)

    st.markdown("### Different Values")
    st.dataframe(different, use_container_width=True)

    st.markdown(f"### Only in {project1}")
    st.dataframe(only_left, use_container_width=True)

    st.markdown(f"### Only in {project2}")
    st.dataframe(only_right, use_container_width=True)


# ---------------------------
# Main App Logic
# ---------------------------

def main_app():
    base_url = "https://lyzy8gvjg8givgo-losadw1.adb.us-phoenix-1.oraclecloudapps.com/ords/los_adw_apex/v1"
    token_url = "https://lyzy8gvjg8givgo-losadw1.adb.us-phoenix-1.oraclecloudapps.com/ords/los_adw_apex/oauth/token"

    client_id = os.getenv("client_id")
    client_secret = os.getenv("client_secret")

    st.title("Project & Well Details")

    if not client_id or not client_secret:
        st.error("Missing env vars: client_id and/or client_secret")
        st.stop()

    mode = st.radio(
        "Select Mode",
        ["Single Project", "Compare Projects"],
        horizontal=True
    )

    try:
        token = get_token(client_id, client_secret, token_url)

        if mode == "Single Project":
            project_number = st.text_input("Enter Project Number")

            if project_number:
                project_data = build_project_dataset(project_number, token, base_url)
                render_single_project(project_data, project_number)

        else:
            col1, col2 = st.columns(2)
            with col1:
                project1 = st.text_input("Enter Project Number 1")
            with col2:
                project2 = st.text_input("Enter Project Number 2")

            if project1 and project2:
                data1 = build_project_dataset(project1, token, base_url)
                data2 = build_project_dataset(project2, token, base_url)
                render_compare_projects(project1, project2, data1, data2)

    except Exception as e:
        st.error(f"Error: {e}")


# ---------------------------
# App Flow Control
# ---------------------------

if __name__ == "__main__":
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login()
    else:
        main_app()
