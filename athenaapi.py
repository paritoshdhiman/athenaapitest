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
    """
    Flattens dict/list JSON. Explodes lists repeatedly until no list columns remain.
    Good for debug visibility (not used for pricing table).
    """
    if raw_json is None:
        return pd.DataFrame()

    df = pd.json_normalize(raw_json, sep=sep)

    # Explode list-valued columns repeatedly
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


# ---------------------------
# Pricing parsers (match your raw JSONs)
# ---------------------------

def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def parse_completion_design(raw, project_number, well_id) -> pd.DataFrame:
    """
    raw = list of design dicts
    each design has proppantsTypeMesh = list of proppant dicts (or empty)
    """
    rows = []
    designs = raw if isinstance(raw, list) else []
    for design_idx, d in enumerate(designs):
        proppants = d.get("proppantsTypeMesh", []) if isinstance(d, dict) else []
        if not proppants:
            continue

        for p in proppants:
            rows.append({
                "project_number": project_number,
                "well_id": str(well_id),
                "source_attribute": "completionDesign",
                "line_type": "proppant",
                "design_idx": design_idx,
                "orden": None,

                "catalog_external": p.get("proppantSizeCatalogExternal"),
                "item_description": p.get("proppantCommercialName"),
                "commercial_name": p.get("proppantCommercialName"),
                "concentration": None,
                "unit": p.get("unit"),

                "unit_price": _f(p.get("unitPrice")),
                "discount_pct": _f(p.get("discountPercentage")),
                "discounted_unit_price": _f(p.get("discountedUnitPrice")),
                "quoted_quantity": _f(p.get("quotedQuantity")),
            })
    return pd.DataFrame(rows)


def parse_frac_chemicals(raw, project_number, well_id) -> pd.DataFrame:
    """
    raw = list of group dicts
    each group has chemTypes = list of chemical dicts
    """
    rows = []
    groups = raw if isinstance(raw, list) else []
    for grp_idx, grp in enumerate(groups):
        chem_types = grp.get("chemTypes", []) if isinstance(grp, dict) else []
        for ch in chem_types:
            rows.append({
                "project_number": project_number,
                "well_id": str(well_id),
                "source_attribute": "fracChemicals",
                "line_type": "chemical",
                "design_idx": grp_idx,
                "orden": None,

                "catalog_external": ch.get("chemicalTypeCatalogExternal"),
                "item_description": ch.get("commercialName"),
                "commercial_name": ch.get("commercialName"),
                "concentration": ch.get("concentration"),
                "unit": ch.get("unit"),

                "unit_price": _f(ch.get("unitPrice")),
                "discount_pct": _f(ch.get("discount")),  # field name is "discount"
                "discounted_unit_price": _f(ch.get("discountedUnitPrice")),
                "quoted_quantity": _f(ch.get("quotedQuantity")),
            })
    return pd.DataFrame(rows)


def parse_cartage_charges(raw, project_number, well_id) -> pd.DataFrame:
    """
    raw = list of cartage dicts
    measurementUnits = {"label": "..."}
    """
    rows = []
    items = raw if isinstance(raw, list) else []
    for it in items:
        mu = it.get("measurementUnits") if isinstance(it, dict) else {}
        rows.append({
            "project_number": project_number,
            "well_id": str(well_id),
            "source_attribute": "cartageCharges",
            "line_type": "cartage",
            "design_idx": None,
            "orden": it.get("orden"),

            "catalog_external": it.get("cartageChargeCatalogExternal"),
            "item_description": it.get("itemDescription"),
            "commercial_name": None,
            "concentration": None,
            "unit": (mu or {}).get("label"),

            "unit_price": _f(it.get("unitPrice")),
            "discount_pct": _f(it.get("discountPercentage")),
            "discounted_unit_price": _f(it.get("discountedUnitPrice")),
            "quoted_quantity": _f(it.get("quotedQuantity")),
        })
    return pd.DataFrame(rows)


def parse_service_charges(raw, project_number, well_id) -> pd.DataFrame:
    """
    raw = list of service charge dicts
    measurementUnits = {"label": "..."}
    """
    rows = []
    items = raw if isinstance(raw, list) else []
    for it in items:
        mu = it.get("measurementUnits") if isinstance(it, dict) else {}
        rows.append({
            "project_number": project_number,
            "well_id": str(well_id),
            "source_attribute": "serviceCharges",
            "line_type": "service",
            "design_idx": None,
            "orden": it.get("orden"),

            "catalog_external": it.get("serviceChargeCatalogExternal"),
            "item_description": it.get("itemDescription"),
            "commercial_name": None,
            "concentration": None,
            "unit": (mu or {}).get("label"),

            "unit_price": _f(it.get("unitPrice")),
            "discount_pct": _f(it.get("discountPercentage")),
            "discounted_unit_price": _f(it.get("discountedUnitPrice")),
            "quoted_quantity": _f(it.get("quotedQuantity")),
        })
    return pd.DataFrame(rows)


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

    project_number = st.text_input("Enter Project Number")

    if project_number:
        try:
            # 1) OAuth Token
            token = get_token(client_id, client_secret, token_url)

            # 2) Project API
            project_url = f"{base_url}/project/?project_number={project_number}"
            raw_project = get_details(project_url, token)
            st.write("Raw Project JSON:")
            st.write(raw_project)

            df_project = pd.json_normalize(raw_project, sep="_")

            # Wells
            if "wellIDs" in df_project.columns:
                df_exploded = df_project[["wellIDs"]].explode("wellIDs").reset_index(drop=True)
                df_wells = pd.json_normalize(df_exploded["wellIDs"])
            else:
                df_wells = pd.DataFrame()

            st.write("Project Details:")
            st.dataframe(df_project.T)

            st.write("Well IDs:")
            st.dataframe(df_wells)

            # Attributes to pull (keep your existing ones)
            attributes = [
                "generalWellInformation",
                "completionDesign",
                "fracChemicals",
                "cartageCharges",
                "serviceCharges",
            ]

            pricing_frames = []

            st.write("Well Information (raw + flattened):")
            if not df_wells.empty and "id" in df_wells.columns:
                for wid in df_wells["id"].dropna().unique():
                    for att in attributes:
                        api_url = f"{base_url}/{att}?well_id={wid}"
                        st.write(api_url)
                        raw = get_details(api_url, token)
                        st.write(raw)

                        df_flat = flatten_all_json(raw)
                        st.write(f"{wid} - {att}")
                        st.dataframe(df_flat)

                        # Build pricing table from the 4 pricing attributes
                        if att == "completionDesign":
                            pricing_frames.append(parse_completion_design(raw, project_number, wid))
                        elif att == "fracChemicals":
                            pricing_frames.append(parse_frac_chemicals(raw, project_number, wid))
                        elif att == "cartageCharges":
                            pricing_frames.append(parse_cartage_charges(raw, project_number, wid))
                        elif att == "serviceCharges":
                            pricing_frames.append(parse_service_charges(raw, project_number, wid))

                # Unified pricing table
                df_pricing_all = pd.concat(
                    [x for x in pricing_frames if x is not None and not x.empty],
                    ignore_index=True
                ) if pricing_frames else pd.DataFrame()

                if not df_pricing_all.empty:
                    # Extended totals
                    df_pricing_all["extended_discounted"] = (
                        df_pricing_all["discounted_unit_price"].fillna(0)
                        * df_pricing_all["quoted_quantity"].fillna(0)
                    )
                    df_pricing_all["extended_list"] = (
                        df_pricing_all["unit_price"].fillna(0)
                        * df_pricing_all["quoted_quantity"].fillna(0)
                    )

                    st.subheader("Pricing + Quoted Quantity (All Wells / All Pricing Attributes)")
                    st.dataframe(df_pricing_all)

                    st.subheader("Totals by Well + Line Type")
                    df_summary = (
                        df_pricing_all
                        .groupby(["well_id", "line_type"], as_index=False)
                        .agg(
                            lines=("line_type", "size"),
                            total_qty=("quoted_quantity", "sum"),
                            total_discounted=("extended_discounted", "sum"),
                            total_list=("extended_list", "sum"),
                        )
                    )
                    st.dataframe(df_summary)
                else:
                    st.info("No pricing line items found (all pricing arrays empty).")
            else:
                st.info("No wells found for this project.")

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
