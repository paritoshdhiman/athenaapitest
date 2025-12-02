import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import streamlit as st

st.set_page_config(layout='wide')

# ---------------------------
# Utility functions
# ---------------------------

def get_token(client_id: str, client_secret: str, token_url: str) -> str:
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'grant_type': 'client_credentials'}
    auth = HTTPBasicAuth(client_id, client_secret)

    resp = requests.post(token_url, headers=headers, data=data, auth=auth)
    resp.raise_for_status()
    payload = resp.json()
    if 'access_token' not in payload:
        raise RuntimeError(f"Unexpected token response: {payload}")
    return payload['access_token']


def get_details(url: str, token: str) -> dict:
    headers = {
        'Authorization': f"Bearer {token}",
        'Accept': 'application/json'
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def flatten_all_json(raw_json: dict, sep: str = '_') -> pd.DataFrame:
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
            df = pd.json_normalize(df.to_dict(orient='records'), sep=sep)

    return df


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
# Credentials Input Page
# ---------------------------

def credential_page():
    st.title("Enter API Credentials")

    if "client_id" not in st.session_state:
        st.session_state.client_id = ""
    if "client_secret" not in st.session_state:
        st.session_state.client_secret = ""

    with st.form("cred_form"):
        client_id = st.text_input("Client ID", value=st.session_state.client_id)
        client_secret = st.text_input("Client Secret", value=st.session_state.client_secret, type="password")
        submit = st.form_submit_button("Save & Continue")

        if submit:
            if client_id.strip() == "" or client_secret.strip() == "":
                st.error("Both Client ID and Client Secret are required.")
            else:
                st.session_state.client_id = client_id
                st.session_state.client_secret = client_secret
                st.session_state.creds_entered = True
                st.rerun()


# ---------------------------
# Main App Logic
# ---------------------------

def main_app():

    base_url = 'https://lyzy8gvjg8givgo-losadw1.adb.us-phoenix-1.oraclecloudapps.com/ords/los_adw_apex/v1'
    token_url = 'https://lyzy8gvjg8givgo-losadw1.adb.us-phoenix-1.oraclecloudapps.com/ords/los_adw_apex/oauth/token'

    st.title("Project & Well Details")

    project_number = st.text_input("Enter Project Number")

    if project_number:
        try:
            # 1. OAuth Token
            token = get_token(
                st.session_state.client_id,
                st.session_state.client_secret,
                token_url
            )

            # 2. Project API
            project_url = f"{base_url}/project/?project_number={project_number}"
            raw = get_details(project_url, token)
            st.write(raw)

            df_project = pd.json_normalize(raw, sep='_')

            # Wells
            if 'wellIDs' in df_project.columns:
                df_exploded = df_project[['wellIDs']].explode('wellIDs').reset_index(drop=True)
                df_wells = pd.json_normalize(df_exploded['wellIDs'])
            else:
                df_wells = pd.DataFrame()

            st.write("Project Details:")
            st.dataframe(df_project.T)

            st.write("Well IDs:")
            st.write(df_wells)

            # Well Attributes
            attributes = [
                "generalWellInformation",
                "completionDesign",
                "fracChemicals",
                "cartageCharges",
                "serviceCharges"
            ]

            st.write("Well Information:")
            if not df_wells.empty and 'id' in df_wells.columns:
                for wid in df_wells['id'].unique():
                    for att in attributes:
                        api_url = f"{base_url}/{att}?well_id={wid}"
                        st.write(api_url)
                        raw = get_details(api_url, token)
                        st.write(raw)
                        df = flatten_all_json(raw)
                        st.write(f"{wid} - {att}")
                        st.write(df)
            else:
                st.info("No wells found for this project.")

        except Exception as e:
            st.error(f"Error: {e}")


# ---------------------------
# App Flow Control
# ---------------------------

if __name__ == "__main__":

    # Step 1 — Login
    if "logged_in" not in st.session_state or not st.session_state.logged_in:
        login()

    # Step 2 — Ask for client credentials
    elif "creds_entered" not in st.session_state or not st.session_state.get("creds_entered", False):
        credential_page()

    # Step 3 — Run main app
    else:
        main_app()
