import numpy as np
import streamlit as st
from streamlit import session_state as ss
import pandas as pd

from backend_functions.credential_management import encrypt_text
from backend_functions.database_functions import get_conn, qec, sql_to_dict
from backend_functions.helper_functions import reverse_key_lookup, list_to_dict_by_key
from backend_functions.logging_functions import log_app_event, start_timer, elapsed_ms


def admin_button_dict():
    d = {"passwords": ":material/key_vertical:",
         "tasks": ":material/checklist:",
         "services": ":material/api:"}
    return d


def render_admin_module():
    st.write("Admin Module")
    abd = admin_button_dict()
    ss.admin_nav_selection = st.pills(label="top_nav_selection",
                                    label_visibility="hidden",
                                    options=list(set(abd.values())),
                                    default=abd.get("home"))

    simple_selection = reverse_key_lookup(abd, ss.admin_nav_selection)
    if simple_selection == 'passwords':
        render_password_submodule()
    elif simple_selection == 'admin':
        st.write("Task Submodule")
    elif simple_selection == 'services':
        render_service_submodule()


def render_service_submodule():
    # Read in any existing services
    t0 = None
    df = pd.read_sql('SELECT * FROM api_services.api_service_list', get_conn(alchemy=True))
    if not df.empty:
        col_config = {"api_service_name": st.column_config.TextColumn(label="Service",
                                                                      pinned=True, disabled=False),
                      "api_service_function": st.column_config.TextColumn(label="Login Functions",
                                                                           pinned=False,
                                                                           disabled=False),
                      "api_credential_requirements": st.column_config.TextColumn(label="Credentials Needed",
                                                                                 pinned=False,
                                                                                 disabled=False)}
        st.write("Known Services")
        st.data_editor(df,
                        hide_index=True,
                        column_config=col_config,
                        key = "service_editor",
                        on_change = handle_service_changes,
                        args = (df,)
        )

    if st.button(":material/add: Add New Service"):
        ss.admin_service_add = True

    if "admin_service_add" in ss and ss.admin_service_add:
        ss.new_service_name = st.text_input(label="Name:")
        ss.new_service_functions = st.text_input(label="Login Functions",
                                              placeholder="comma separated list")
        ss.new_credential_requirements = st.text_input(label="Credential Requirements",
                                              placeholder="comma separated list")
        if st.button(":material/save: Submit"):
            ss.new_service_submission = True
            t0 = start_timer()

    if "new_service_submission" in ss and ss.new_service_submission:
        insert_sql = """INSERT INTO api_services.api_service_list (api_service_name, 
                       api_service_function, api_credential_requirements)
                       VALUES (%s, %s, %s);"""
        params = (ss.new_service_name, ss.new_service_functions.lower(), ss.new_credential_requirements.lower())
        qec(insert_sql, params)
        log_app_event(cat="Admin", desc=f"New Service Creation: {ss.new_service_name}", exec_time=elapsed_ms(t0))
        ss.new_service_submission = False
        ss.admin_service_add = False
        st.rerun()


def handle_service_changes(original_df):
    # used in conjunction with data editor, records changes to postgres

    t0 = start_timer()

    # Get the edited data from session state
    edited_data = ss.service_editor

    # Convert to DataFrame if it's a dict with edited_rows metadata
    if isinstance(edited_data, dict):
        # st.data_editor returns edited data in a special format
        # Use the 'edited_rows' key to get actual changes
        if 'edited_rows' in edited_data and edited_data['edited_rows']:
            edited_rows = edited_data['edited_rows']

            update_sql = """
                    UPDATE api_services.api_service_list 
                    SET api_service_function = %s,
                        api_credential_requirements = %s
                    WHERE api_service_name = %s;
                """

            # edited_rows is a dict where keys are row indices
            for row_idx, changes in edited_rows.items():
                # Get the service name from original df
                service_name = original_df.iloc[row_idx]['api_service_name']

                # Get updated values (use original if not changed)
                api_functions = changes.get('api_service_function',
                                            original_df.iloc[row_idx]['api_service_function'])
                api_credentials = changes.get('api_credential_requirements',
                                              original_df.iloc[row_idx]['api_credential_requirements'])

                params = (api_functions, api_credentials, service_name)
                qec(update_sql, params)

            log_app_event(
                cat="Admin",
                desc=f"Service Updates: {len(edited_rows)} rows changed",
                exec_time=elapsed_ms(t0)
            )
        return

    # If it's already a DataFrame (older Streamlit versions)
    edited_df = edited_data

    if original_df.equals(edited_df):
        return

    check_cols = ['api_service_function', 'api_credential_requirements']

    # Find changed rows
    changed_indices = []
    for idx in range(len(edited_df)):
        for col in check_cols:
            orig_val = str(original_df.iloc[idx][col]) if pd.notna(original_df.iloc[idx][col]) else ''
            edit_val = str(edited_df.iloc[idx][col]) if pd.notna(edited_df.iloc[idx][col]) else ''
            if orig_val != edit_val:
                changed_indices.append(idx)
                break

    if not changed_indices:
        return

    update_sql = """
            UPDATE api_services.api_service_list 
            SET api_service_function = %s,
                api_credential_requirements = %s
            WHERE api_service_name = %s;
        """

    for idx in changed_indices:
        params = (
            edited_df.iloc[idx]['api_service_function'],
            edited_df.iloc[idx]['api_credential_requirements'],
            edited_df.iloc[idx]['api_service_name']
        )
        qec(update_sql, params)

    log_app_event(
        cat="Admin",
        desc=f"Service Updates: {len(changed_indices)} rows changed",
        exec_time=elapsed_ms(t0)
    )



def render_password_submodule():
    # Enables the user to store the credentials required for a specific service

    # Get the list of credentials into a dictionary
    cred_sql = """SELECT api_service_name, api_credential_requirements from api_services.api_service_list"""
    service_dict = list_to_dict_by_key(list_of_dicts=sql_to_dict(cred_sql),
                                       primary_key="api_service_name")

    # Step 1: Let user pick a service
    service_list = list(service_dict.keys())
    selected_service = st.selectbox("Select a service", service_list)

    # Step 2: Extract credential fields for that service
    creds = [c.strip() for c in service_dict[selected_service].split(",") if c.strip()]

    # Step 3: Dynamically generate inputs
    user_inputs = {}
    st.subheader(f"Enter credentials for {selected_service}")
    for cred in creds:
        user_inputs[cred] = st.text_input(cred)

    # Step 4: Return structure only when all credentials are filled
    if all(user_inputs.values()):
        t0 = start_timer()
        st.success(f"All credentials captured for {selected_service}.")
        enc_input = encrypt_text(user_inputs)
        insert_sql = """INSERT INTO api_services.credentials (api_service_name, api_credentials)
                         VALUES (%s, %s);"""
        params = (selected_service, enc_input)
        qec(insert_sql, params)
        log_app_event(cat='Admin', desc=f"Credential Saved: {selected_service}", exec_time=elapsed_ms(t0))
        # Save encrypted results

    return None