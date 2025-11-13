import streamlit as st
from streamlit import session_state as ss
import pandas as pd
from backend_functions.database_functions import get_conn, qec
from backend_functions.helper_functions import reverse_key_lookup
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


def render_password_submodule():
    # Get service list
    # Obtain Credentials
    st.write("dependency on services")


def render_service_submodule():
    # Read in any existing services
    t0 = None
    df = pd.read_sql('SELECT * FROM api_services.api_service_list', get_conn(alchemy=True))
    if not df.empty:
        col_config = {"api_service_name": st.column_config.TextColumn(label="Service",
                                                                      pinned=True, disabled=True),
                      "api_service_functions": st.column_config.TextColumn(label="Login Functions",
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
                       api_service_functions, api_credential_requirements)
                       VALUES (%s, %s, %s);"""
        params = (ss.new_service_name.lower(), ss.new_service_functions.lower(), ss.new_credential_requirements.lower())
        qec(insert_sql, params)
        log_app_event(cat="Admin", desc=f"New Service Creation: {ss.new_service_name}", exec_time=elapsed_ms(t0))
        ss.new_service_submission = False
        ss.admin_service_add = False


def handle_service_changes(orig_df):
    # used in conjunction with data editor, records changes to postgres
    t0 = start_timer()

    # Get the edited dataframe from session state using the key
    edited_df = ss.service_editor

    # Find rows that have changed
    changed_mask = (orig_df != edited_df).any(axis=1)
    changed_rows = edited_df[changed_mask]

    if changed_rows.empty:
        return

    # Update each changed row
    update_sql = """
        UPDATE api_services.api_service_list 
        SET api_service_functions = %s,
            api_credential_requirements = %s
        WHERE api_service_name = %s;
    """

    for _, row in changed_rows.iterrows():
        params = (
            row['api_service_functions'],
            row['api_credential_requirements'],
            row['api_service_name']
        )
        qec(update_sql, params)

    log_app_event(
        cat="Admin",
        desc=f"Service Updates: {len(changed_rows)} rows changed",
        exec_time=elapsed_ms(t0)
    )
