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
    Debug helper: flattens dict/list JSON and repeatedly explodes list columns.
    """
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


# ---------------------------
# Pricing parsers (as you specified)
# ---------------------------

def _f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def parse_completion_design_items(raw, project_number, well_id) -> pd.DataFrame:
    """
    completionDesign raw:
      list of design dicts, each has proppantsTypeMesh: [ {...} ]
    Map to:
      item_code, name, uom, unit_price, discount_percentage, discounted_unit_price, quantity
    """
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
    """
    fracChemicals raw:
      list of group dicts: [{"chemTypes":[{...}], ...}, ...]
    chemTypes -> rows
    Map to:
      chemicalTypeCatalogExternal as item_code
      commercialName as name
      unit as uom
      unitPrice
      discount as discount_percentage
      discountedUnitPrice
      quotedQuantity as quantity
    """
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
                "discount_percentage": _f(ch.get("discount")),  # note: field is "discount"
                "discounted_unit_price": _f(ch.get("discountedUnitPrice")),
                "quantity": _f(ch.get("quotedQuantity")),
            })
    return pd.DataFrame(rows)


def parse_cartage_charges_items(raw, project_number, well_id) -> pd.DataFrame:
    """
    cartageCharges raw: list of dicts
    measurementUnits: {"label":"LBS"} -> uom
    Map:
      cartageChargeCatalogExternal as item_code
      itemDescription as name
      unitPrice, discountPercentage, discountedUnitPrice, quotedQuantity
    """
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
    """
    serviceCharges raw: list of dicts
    measurementUnits: {"label":"DAY"} -> uom
    Map:
      serviceChargeCatalogExternal as item_code
      itemDescription as name
      unitPrice, discountPercentage, discountedUnitPrice, quotedQuantity
    """
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

            # Keep your existing debug attributes
            debug_attributes = [
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

                    for att in debug_attributes:
                        api_url = f"{base_url}/{att}?well_id={wid}"
                        st.write(api_url)
                        raw = get_details(api_url, token)
                        st.write(raw)

                        df_flat = flatten_all_json(raw)
                        st.write(f"{wid} - {att}")
                        st.dataframe(df_flat)

                        # Build pricing items table
                        if att == "completionDesign":
                            pricing_frames.append(parse_completion_design_items(raw, project_number, wid))
                        elif att == "fracChemicals":
                            pricing_frames.append(parse_frac_chemicals_items(raw, project_number, wid))
                        elif att == "cartageCharges":
                            pricing_frames.append(parse_cartage_charges_items(raw, project_number, wid))
                        elif att == "serviceCharges":
                            pricing_frames.append(parse_service_charges_items(raw, project_number, wid))

                # Unified pricing table
                df_items = (
                    pd.concat([x for x in pricing_frames if x is not None and not x.empty], ignore_index=True)
                    if pricing_frames else pd.DataFrame()
                )

                if not df_items.empty:
                    # Useful computed fields
                    df_items["extended_discounted"] = df_items["discounted_unit_price"].fillna(0) * df_items["quantity"].fillna(0)
                    df_items["extended_list"] = df_items["unit_price"].fillna(0) * df_items["quantity"].fillna(0)

                    # Optional: stable column order
                    col_order = [
                        "project_number", "well_id", "source",
                        "item_code", "name", "uom",
                        "unit_price", "discount_percentage", "discounted_unit_price",
                        "quantity", "extended_discounted", "extended_list"
                    ]
                    df_items = df_items[[c for c in col_order if c in df_items.columns]]

                    st.subheader("All Pricing Items (All Wells)")
                    st.dataframe(df_items)

                    st.subheader("Totals by Well + Source")
                    df_summary = (
                        df_items
                        .groupby(["well_id", "source"], as_index=False)
                        .agg(
                            lines=("item_code", "size"),
                            total_qty=("quantity", "sum"),
                            total_discounted=("extended_discounted", "sum"),
                            total_list=("extended_list", "sum"),
                        )
                    )
                    st.dataframe(df_summary)

                else:
                    st.info("No pricing items found (all pricing arrays empty).")

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
